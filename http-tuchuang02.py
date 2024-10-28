import os
import json
from fastapi import FastAPI, HTTPException, Depends, File, UploadFile
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import FileResponse
from apscheduler.schedulers.background import BackgroundScheduler
from typing import Optional
from starlette.middleware.sessions import SessionMiddleware
from starlette.requests import Request
import uvicorn
import secrets


class ImageHostingServer:
    def __init__(self, host="127.0.0.1", port=8001, use_https=False, username="admin", password="admin"):
        self.app = FastAPI()
        self.host = host
        self.port = port
        self.use_https = use_https
        self.username = username
        self.password_hash = secrets.compare_digest(secrets.token_urlsafe(16), password)  # 密码加密
        self.security = HTTPBasic()
        self.resource_dir = "resources"
        os.makedirs(self.resource_dir, exist_ok=True)

        # 添加会话支持（用于首页认证）
        self.app.add_middleware(SessionMiddleware, secret_key="supersecretkey")

        # 路由配置
        self.setup_routes()

        # 定时任务
        self.scheduler = BackgroundScheduler()
        self.scheduler.add_job(self.refresh_resource_list, 'interval', minutes=30)
        self.scheduler.start()

    def verify_credentials(self, credentials: HTTPBasicCredentials = Depends(HTTPBasic)):
        correct_username = secrets.compare_digest(credentials.username, self.username)
        correct_password = secrets.compare_digest(credentials.password, self.password_hash)
        if not (correct_username and correct_password):
            raise HTTPException(status_code=401, detail="Unauthorized")
        return credentials.username

    def setup_routes(self):
        # 首页 (需要认证)
        @self.app.get("/")
        async def home(credentials: HTTPBasicCredentials = Depends(self.verify_credentials)):
            return {"message": "Welcome to the Image Hosting Server"}

        # 上传文件接口
        @self.app.post("/upload")
        async def upload_file(file: UploadFile = File(...), file_type: str = "others"):
            save_dir = os.path.join(self.resource_dir, file_type)
            os.makedirs(save_dir, exist_ok=True)
            file_path = os.path.join(save_dir, file.filename)
            with open(file_path, "wb") as f:
                f.write(await file.read())
            return {"message": "File uploaded", "url": f"/{file_type}/{file.filename}"}

        # 下载文件接口
        @self.app.get("/{file_type}/{filename}")
        async def download_file(file_type: str, filename: str):
            directory = os.path.join(self.resource_dir, file_type)
            file_path = os.path.join(directory, filename)
            if not os.path.exists(file_path):
                raise HTTPException(status_code=404, detail="File not found")
            return FileResponse(file_path)

        # 获取资源列表
        @self.app.get("/resources")
        async def get_resources():
            with open(os.path.join(self.resource_dir, "resources.json"), "r") as f:
                resources = json.load(f)
            return resources

        # 刷新资源目录并生成 JSON 文件
        self.refresh_resource_list()

    def refresh_resource_list(self):
        resources = {}
        for root, dirs, files in os.walk(self.resource_dir):
            for file in files:
                file_path = os.path.relpath(os.path.join(root, file), self.resource_dir)
                resources[file] = file_path
        with open(os.path.join(self.resource_dir, "resources.json"), "w") as f:
            json.dump(resources, f)

    def run(self):
        ssl_config = {"ssl_keyfile": "key.pem", "ssl_certfile": "cert.pem"} if self.use_https else None
        uvicorn.run(self.app, host=self.host, port=self.port,
                    ssl_keyfile=ssl_config.get("ssl_keyfile") if ssl_config else None,
                    ssl_certfile=ssl_config.get("ssl_certfile") if ssl_config else None)


# 示例调用
if __name__ == "__main__":
    server = ImageHostingServer(host="0.0.0.0", use_https=False, username="admin", password="admin")
    server.run()