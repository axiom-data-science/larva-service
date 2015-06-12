from larva_service import app

if __name__ == '__main__':
    app.debug = True
    app.run('127.0.0.1', 8000)
