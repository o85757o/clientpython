import os
import json
from flask import Flask, request, jsonify, send_from_directory
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash
from apscheduler.schedulers.background import BackgroundScheduler


class ImageHostingServer:
    def __init__(self, host="127.0.0.1", port=5000, use_https=False, username="admin", password="admin"):
        self.app = Flask(__name__)
        self.host = host
        self.port = port
        self.use_https = use_https
        self.username = username
        self.auth = HTTPBasicAuth()

        # 存储资源文件夹路径
        self.resource_dir = "resources"
        os.makedirs( self.resource_dir, exist_ok=True)

        # 设置认证
        self.users = {username: generate_password_hash(password)}
        self.auth.verify_password(self.verify_password)

        # 注册路由
        self.setup_routes()

        # 定时任务
        self.scheduler = BackgroundScheduler()
        self.scheduler.add_job(self.refresh_resource_list, 'interval', minutes=30)
        self.scheduler.start()

    def verify_password(self, username, password):
        if username in self.users and check_password_hash(self.users.get(username), password):
            return username

    def setup_routes(self):
        # 首页
        @self.app.route("/")
        @self.auth.login_required
        def home():
            return "Welcome to Image Hosting Server"

        # 上传文件接口
        @self.app.route("/upload", methods=["POST"])
        def upload_file():
            file = request.files.get("file")
            if not file:
                return jsonify({"error": "No file uploaded"}), 400
            file_type = request.args.get("type", "others")
            save_dir = os.path.join(self.resource_dir, file_type)
            os.makedirs(save_dir, exist_ok=True)
            file_path = os.path.join(save_dir, file.filename)
            file.save(file_path)
            return jsonify({"message": "File uploaded", "url": f"/{file_type}/{file.filename}"}), 201

        # 下载文件接口
        @self.app.route("/<file_type>/<filename>", methods=["GET"])
        def download_file(file_type, filename):
            directory = os.path.join(self.resource_dir, file_type)
            return send_from_directory(directory, filename)

        # 获取资源列表
        @self.app.route("/resources", methods=["GET"])
        def get_resources():
            with open(os.path.join(self.resource_dir, "resources.json"), "r") as f:
                resources = json.load(f)
            return jsonify(resources)

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
        if self.use_https:
            self.app.run(host=self.host, port=self.port, ssl_context="adhoc")
        else:
            self.app.run(host=self.host, port=self.port)


# 示例调用
if __name__ == "__main__":
    server = ImageHostingServer(host="0.0.0.0", use_https=False, username="admin", password="admin")
    server.run()