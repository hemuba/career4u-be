import json
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

lista = [6, 4, 5, 4.5, 4, 3.5, 2, 5, 5.25, 4.5, 4.5, 5, 5.5, 1.5, 7]

lista_2 = [5, 4.5, 3, 3.5, 4, 3.65, 3.5, 2.5, 3, 2, 3.5, 3.5, 3, 4.5, 5, 4.5, 4, 3.5, 3.5, 3.5, 4, 5.5, 4, 3.5, 4.5, 3.5, 3.5, 4, 4.5]

def get_ma(f_lista: list[float]) -> float:
    return sum(f_lista) / len(f_lista)


def get_std_dev(lista:list[float], ma):
    sommatoria_scarti = 0
    for n in lista:
        sommatoria_scarti += (n - ma) ** 2
        
    varianza = sommatoria_scarti / (len(lista) -1)
    std_dev = varianza **0.5
    return std_dev


ma_1= get_ma(lista)
ma_2 = get_ma(lista_2)

std_dev_1 = get_std_dev(lista, ma_1)
std_dev_2 = get_std_dev(lista_2, ma_2)


app = FastAPI()

# CORS: permette richieste da file locale o da http://127.0.0.1:5500 ecc.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # in prod restringi
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/stats")

def get_stats():
    return {
        "ma_aug": round(ma_1, 2),
        "STD_DEV_aug": round(std_dev_1, 2),
        "MA_sep": round(ma_2, 2),
        "STD_DEV_sep": round(std_dev_2, 2)
        
    }

if __name__ == "__main__":
    uvicorn.run("main:app", reload=True)