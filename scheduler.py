"""
Scheduler — ETL Socios automático
Corre el pipeline cada lunes a las 8:00 AM.
Autor: Dahyana

Para ejecutar en segundo plano:
  python3 scheduler.py &
"""

import schedule
import time
from datetime import datetime
from etl_pipeline import main as correr_etl


def job():
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Scheduler: iniciando corrida automática...")
    try:
        correr_etl()
        print("  ✔ Corrida completada exitosamente.")
    except Exception as e:
        print(f"  ✖ Error en la corrida: {e}")


# Programar: cada lunes a las 08:00
schedule.every().monday.at("08:00").do(job)

print("═" * 50)
print("  SCHEDULER ETL SOCIOS — Activo")
print("  Próxima corrida: lunes 08:00 AM")
print("  Ctrl+C para detener")
print("═" * 50)

# Para demo inmediata, descomentar:
# job()

while True:
    schedule.run_pending()
    time.sleep(60)
