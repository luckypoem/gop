#!/usr/bin/env python
# coding:utf-8

import sys
import os

sys.dont_write_bytecode = True

if sys.version > '3.':
    sys.exit(sys.stderr.write('Please run uploader.py by python2\n'))

sys.stderr.write('Loading Google Appengine SDK...')
os.chdir(os.path.abspath(os.path.dirname(__file__)))

import re
import getpass
import socket
import ssl
import mimetypes

mimetypes._winreg = None

def clear():
    if os.name == 'nt':
        os.system('cls')
    else:
        sys.stderr.write("\x1b[2J\x1b[H")

def println(s, file=sys.stderr):
    assert type(s) is type(u'')
    file.write(s.encode(sys.getfilesystemencoding(), 'replace') + os.linesep)

try:
    socket.create_connection(('127.0.0.1', 8087), timeout=1).close()
    os.environ['HTTP_PROXY'] = 'http://127.0.0.1:8087'
    os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:8087'
except socket.error:
    clear()
    println(u'警告：建议先启动 GoProxy 客户端或者 VPN 然后再上传，如果您的 VPN 已经打开的话，请按回车键继续。')
    println(u'注意：上传时请保持 GoProxy 在默认端口 (8087) 监听。')
    raw_input()


_realgetpass = getpass.getpass
def getpass_getpass(prompt='Password:', stream=None):
    try:
        import msvcrt
    except ImportError:
        return _realgetpass(prompt, stream)
    password = ''
    sys.stdout.write(prompt)
    while True:
        ch = msvcrt.getch()
        if ch == '\b':
            if password:
                password = password[:-1]
                sys.stdout.write('\b \b')
            else:
                continue
        elif ch == '\r':
            sys.stdout.write(os.linesep)
            return password
        else:
            password += ch
            sys.stdout.write('*')
getpass.getpass = getpass_getpass


sys.path = ['google_appengine'] + sys.path
try:
    filename = './google_appengine/google/appengine/tools/appengine_rpc_httplib2.py'
    with open(filename, 'rb') as fp:
        text = fp.read()
    if '~/.appcfg_oauth2_tokens' in text:
        with open(filename, 'wb') as fp:
            fp.write(text.replace('~/.appcfg_oauth2_tokens', './.appcfg_oauth2_tokens'))
except Exception:
    pass

import httplib2
def _ssl_wrap_socket(sock, key_file, cert_file,
                     disable_validation, ca_certs):
    cert_reqs = ssl.CERT_NONE
    return ssl.wrap_socket(sock, keyfile=key_file, certfile=cert_file,
                           cert_reqs=ssl.CERT_NONE, ca_certs=None,
                           ssl_version=ssl.PROTOCOL_TLSv1)
httplib2._ssl_wrap_socket = _ssl_wrap_socket
httplib2.HTTPSConnectionWithTimeout._ValidateCertificateHostname = lambda a, b, c: True
if hasattr(ssl, '_create_unverified_context'):
    setattr(ssl, '_create_default_https_context', ssl._create_unverified_context)

from google_appengine.google.appengine.tools import appengine_rpc, appcfg

def upload(dirname, appid):
    assert isinstance(dirname, basestring) and isinstance(appid, basestring)
    filename = os.path.join(dirname, 'app.yaml')
    with open(filename, 'rb') as fp:
        content = fp.read()
    with open(filename, 'wb') as fp:
        fp.write(re.sub(r'application:\s*\S+', 'application: '+appid, content))
    if os.name == 'nt':
        appcfg.main(['appcfg', 'rollback', dirname])
        appcfg.main(['appcfg', 'update', dirname])
    else:
        appcfg.main(['appcfg', 'rollback', '--noauth_local_webserver', dirname])
        appcfg.main(['appcfg', 'update', '--noauth_local_webserver', dirname])


def main():
    appids = raw_input('APPID:')
    if not re.match(r'[0-9a-zA-Z\-|]+', appids):
        println(u'错误的 appid 格式，请登录 http://appengine.google.com 查看您的 appid!')
        sys.exit(-1)
    if any(x in appids.lower() for x in ('ios', 'android', 'mobile')):
        println(u'appid 不能包含 ios/android/mobile 等字样。')
        sys.exit(-1)
    for appid in appids.split('|'):
        upload('gae', appid)


if __name__ == '__main__':
    clear()
    println(u'''\
===============================================================
 GoProxy 服务端部署程序, 开始上传 gae 应用文件夹
 Linux/Mac 用户, 请使用 python uploader.py 来上传应用
===============================================================

请输入您的appid, 多个appid请用|号隔开
注意：appid 请勿包含 ios/android/mobile 等字样，否则可能被某些网站识别成移动设备。
        '''.strip())
    main()
    println(os.linesep + u'上传成功，请不要忘记编辑 gae.user.json 把你的appid填进去，谢谢。按回车键退出程序。')
    raw_input()
