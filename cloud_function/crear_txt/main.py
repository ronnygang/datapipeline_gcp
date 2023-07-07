import random
from google.cloud import storage
import json

def generate_numbers(request):
    request_json = request.get_json()
    if request_json and 'name' in request_json:
        name = request_json['name']
    else:
        return json.dumps({"error": "No se proporcionó el parámetro 'name' en el cuerpo de la solicitud."}), 400

    # Concatena la fecha con el nombre del archivo
    FILE_NAME = f'numbers_{name}.txt'
    BUCKET = 'dev-ronny-datalake-raw'

    # Genera 50 números aleatorios
    numbers = [random.randint(1, 100) for _ in range(50)]

    # Convierte los números a una cadena de texto
    numbers_str = '\n'.join(str(n) for n in numbers)

    # Crea un cliente de Cloud Storage
    client = storage.Client()

    # Obtiene el bucket donde se almacenará el archivo
    bucket = client.get_bucket(BUCKET)

    # Crea un objeto Blob para el archivo
    blob = bucket.blob(FILE_NAME)

    # Guarda los números en el archivo
    blob.upload_from_string(numbers_str)

    # Mensaje de éxito
    message = f'El archivo {FILE_NAME} se creó con éxito en el bucket: {BUCKET}'

    # Retorna el mensaje de éxito en formato JSON
    return json.dumps({"message": message})