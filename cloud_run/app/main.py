from fastapi import FastAPI
import requests

app = FastAPI()

CLOUD_FUNCTION_URL_CAMPAIGN = "https://us-central1-ronny-dev-airflow.cloudfunctions.net/function-create-campaigns"
CLOUD_FUNCTION_URL_TRX = "https://us-central1-ronny-dev-airflow.cloudfunctions.net/function-create-trx"

def execute_function(url, quantity):        
    payload = {"quantity": quantity}
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        return response.json()
    else:
        return {"message": "Error Cloud Function"}

@app.post("/create/{name}")
async def invoke_cloud_function(name: str, quantity: int):
    if name.upper() == 'CAMPAIGNS':
        return execute_function(CLOUD_FUNCTION_URL_CAMPAIGN, quantity)
    elif name.upper() == 'TRANSACTIONS':
        return execute_function(CLOUD_FUNCTION_URL_TRX, quantity)
    else:
        return {"message": "Don't exist this functionality"}
       
@app.get("/")
def read_root():
    return {"Hello": "World"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)