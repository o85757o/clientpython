import json
import os
import subprocess

# 配置文件路径
CONFIG_PATH = "nginx_config.json"
NGINX_CONF_PATH = "nginx.conf"  # Nginx配置文件路径，可能因系统不同而异
NGINX_SERVICE = "nginx"  # Nginx服务名称

# 读取配置文件
def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as file:
            return json.load(file)
    return {}

# 保存配置文件
def save_config(config):
    with open(CONFIG_PATH, "w") as file:
        json.dump(config, file, indent=4)

# 生成nginx.conf配置文件内容
def generate_nginx_conf(config):
    protocol = config.get("protocol", "http")
    tls_version = config.get("tls_version", "TLSv1.2")
    listen_ip = config.get("listen_ip", "0.0.0.0")
    listen_port = config.get("listen_port", 80)
    pass_ip = config.get("pass_ip", "127.0.0.1")
    pass_port = config.get("pass_port", 8080)
    cert_path = config.get("cert_path", "/etc/nginx/cert.pem")
    key_path = config.get("key_path", "/etc/nginx/key.pem")
    additional_tls_options = config.get("tls_options", "")

    # 基础配置
    conf = f"""
    worker_processes 1;
    events {{
        worker_connections 1024;
    }}

    stream {{
        server {{
            listen {listen_ip}:{listen_port} {'ssl' if protocol in ['tcp', 'http2'] and 'tls' in tls_version else ''};
            proxy_pass {pass_ip}:{pass_port};
    """

    # TLS配置
    if "tls" in protocol or "http2" in protocol:
        conf += f"""
            ssl_protocols {tls_version};
            ssl_certificate {cert_path};
            ssl_certificate_key {key_path};
            {additional_tls_options}
        """

    conf += "    }\n}"

    # HTTP配置
    if protocol in ["http", "http2"]:
        conf += f"""
    http {{
        server {{
            listen {listen_ip}:{listen_port} {'ssl http2' if protocol == 'http2' else ''};
            location / {{
                proxy_pass http://{pass_ip}:{pass_port};
            }}
        }}
    }}
    """

    return conf

# 写入nginx.conf配置文件
def write_nginx_conf(config):
    conf_content = generate_nginx_conf(config)
    with open(NGINX_CONF_PATH, "w") as file:
        file.write(conf_content)

# 重新加载nginx服务
def reload_nginx():
    subprocess.run(["systemctl", "reload", NGINX_SERVICE])

# 主函数
def main():
    # 载入或初始化配置
    config = load_config()
    print("当前配置:", config)

    # 更新配置
    config["protocol"] = input("选择协议 (tcp/http/http2): ")
    config["tls_version"] = input("选择TLS版本 (none/tls1.1/tls1.2/tls1.3): ")
    config["listen_ip"] = input("监听IP (默认为0.0.0.0): ") or "0.0.0.0"
    config["listen_port"] = int(input("监听端口 (默认为80): ") or 80)
    config["pass_ip"] = input("转发IP (默认为127.0.0.1): ") or "127.0.0.1"
    config["pass_port"] = int(input("转发端口 (默认为8080): ") or 8080)
    config["cert_path"] = input("证书路径 (默认为/etc/nginx/cert.pem): ") or "/etc/nginx/cert.pem"
    config["key_path"] = input("证书密钥路径 (默认为/etc/nginx/key.pem): ") or "/etc/nginx/key.pem"
    config["tls_options"] = input("其他TLS选项（可选）: ")

    # 保存配置文件
    save_config(config)

    # 生成并写入nginx.conf文件
    write_nginx_conf(config)

    # 重载nginx
    reload_nginx()
    print("Nginx配置已更新并重新加载。")

if __name__ == "__main__":
    main()