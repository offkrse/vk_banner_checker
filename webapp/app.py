from fastapi import FastAPI, Request, Response, Depends, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader
import pathlib, json, os
from itsdangerous import URLSafeTimedSerializer

# ==== Настройки ====
APP_DIR = pathlib.Path("/opt/vk_checker/webapp")
USER_DIR = pathlib.Path("/opt/vk_checker/user")
USER_DIR.mkdir(parents=True, exist_ok=True)

BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "replace_me")
SECRET_KEY = os.getenv("VK_APP_SECRET", "very_secret_key")
serializer = URLSafeTimedSerializer(SECRET_KEY)

# ==== FastAPI ====
app = FastAPI(title="VK Checker Mini App")
templates = Environment(loader=FileSystemLoader(str(APP_DIR / "templates")))

app.mount("/static", StaticFiles(directory=APP_DIR / "static"), name="static")

# ==== Вспомогательные ====
def get_user_path(uid: str) -> pathlib.Path:
    return USER_DIR / f"{uid}.json"

def load_user(uid: str) -> dict | None:
    p = get_user_path(uid)
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return None

def save_user(user: dict):
    p = get_user_path(str(user["telegram_id"]))
    p.write_text(json.dumps(user, ensure_ascii=False, indent=2), encoding="utf-8")

def create_token(telegram_id: int) -> str:
    return serializer.dumps({"telegram_id": telegram_id})

def verify_token(token: str) -> dict | None:
    try:
        return serializer.loads(token, max_age=60 * 60 * 24 * 30)  # 30 дней
    except Exception:
        return None


# ==== Middleware / dependency ====
async def get_current_user(request: Request):
    token = request.cookies.get("vk_session")
    if not token:
        raise HTTPException(401, "Нет сессии")
    data = verify_token(token)
    if not data:
        raise HTTPException(401, "Сессия недействительна")
    uid = str(data["telegram_id"])
    user = load_user(uid)
    if not user:
        raise HTTPException(404, "Пользователь не найден")
    return user


# ==== Маршруты ====

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    token = request.cookies.get("vk_session")
    if token and verify_token(token):
        data = verify_token(token)
        user = load_user(str(data["telegram_id"]))
        if user:
            return templates.get_template("index.html").render(user=user)
    # если не авторизован — на login
    return templates.get_template("login.html").render(title="Авторизация")


@app.post("/api/login")
async def login(request: Request, response: Response):
    data = await request.json()
    uid = str(data.get("telegram_id"))
    name = data.get("name", "User")

    if not uid or not uid.isdigit():
        raise HTTPException(400, "Некорректный ID")

    user = load_user(uid)
    if not user:
        user = {
            "telegram_id": int(uid),
            "name": name,
            "chat_id": uid,
            "cabinets": []
        }
        save_user(user)

    token = create_token(int(uid))
    response.set_cookie("vk_session", token, httponly=True, secure=False)
    return {"ok": True, "message": f"Добро пожаловать, {name}!"}


@app.get("/cabinet/{cabinet_id}", response_class=HTMLResponse)
async def cabinet_page(request: Request, cabinet_id: int, user=Depends(get_current_user)):
    cab = next((c for c in user["cabinets"] if c["id"] == cabinet_id), None)
    if not cab:
        return HTMLResponse("Кабинет не найден", status_code=404)
    return templates.get_template("cabinet.html").render(user=user, cabinet=cab)


@app.post("/api/add_campaigns/{cabinet_id}")
async def add_campaigns(cabinet_id: int, request: Request, user=Depends(get_current_user)):
    data = await request.json()
    lines = [x.strip() for x in data.get("campaigns", "").splitlines() if x.strip().isdigit()]
    added = 0
    for c in user["cabinets"]:
        if c["id"] == cabinet_id:
            for l in lines:
                if int(l) not in c.get("allowed_campaigns", []):
                    c.setdefault("allowed_campaigns", []).append(int(l))
                    added += 1
    save_user(user)
    return {"message": f"Добавлено {added} кампаний"}


@app.post("/api/toggle/{cabinet_id}")
async def toggle_cabinet(cabinet_id: int, user=Depends(get_current_user)):
    for c in user["cabinets"]:
        if c["id"] == cabinet_id:
            c["active"] = not c.get("active", True)
            save_user(user)
            return {"message": f"Статус: {'🟢 Активен' if c['active'] else '🔴 Отключен'}"}
    return {"message": "Кабинет не найден"}
