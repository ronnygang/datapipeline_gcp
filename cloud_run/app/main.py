from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import requests

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

CLOUD_FUNCTION_CAMPAIGNS_CSV = 'https://us-central1-ronny-dev-airflow.cloudfunctions.net/function-create-campaigns-csv'
CLOUD_FUNCTION_TRANSACTIONS_CSV = 'https://us-central1-ronny-dev-airflow.cloudfunctions.net/function-create-transactions-csv'
CLOUD_FUNCTION_CAMPAIGNS_TXT = 'https://us-central1-ronny-dev-airflow.cloudfunctions.net/function-create-campaigns-txt'
CLOUD_FUNCTION_TRANSACTIONS_TXT = 'https://us-central1-ronny-dev-airflow.cloudfunctions.net/function-create-transactions-txt'

def execute_function(url, quantity):        
    payload = {"quantity": quantity}
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        return response.json()
    else:
        return {"message": "Error Cloud Function"}

@app.post("/create/{name}")
async def invoke_cloud_function(name: str, quantity: int):
    if name.upper() == 'CAMPAIGNS_CSV':
        return execute_function(CLOUD_FUNCTION_CAMPAIGNS_CSV, quantity)
    elif name.upper() == 'TRANSACTIONS_CSV':
        return execute_function(CLOUD_FUNCTION_TRANSACTIONS_CSV, quantity)
    elif name.upper() == 'CAMPAIGNS_TXT':
        return execute_function(CLOUD_FUNCTION_CAMPAIGNS_TXT, quantity)
    elif name.upper() == 'TRANSACTIONS_TXT':
        return execute_function(CLOUD_FUNCTION_TRANSACTIONS_TXT, quantity)
    else:
        return {"message": "Don't exist this functionality"}
       

@app.get("/", response_class=HTMLResponse)
def read_root():
    return """
        <html>
            <head>
                <title>Bienvenido a nuestro servicio de BigData</title>
                <link rel="stylesheet" href="/static/style.css">
            </head>
            <body>
                <div class="container">
                    <h1>Bienvenido a nuestro servicio de BigData</h1>
                    <p>Gracias por visitarnos. ¡Esperamos que disfrutes de nuestra plataforma!</p>
                </div>
            </body>
        </html>
    """

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)