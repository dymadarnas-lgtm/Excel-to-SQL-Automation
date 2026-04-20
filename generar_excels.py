"""
Generador de Excels sucios — Proyecto 3
Crea 3 archivos simulando distintas sucursales con errores reales.
"""
import pandas as pd
import os

os.makedirs("../data/excels", exist_ok=True)

# ── SUCURSAL CÓRDOBA ──────────────────────────────────────
cordoba = pd.DataFrame([
    ["Juan",    "García",    "28456789", "1145678901", "juan.garcia@gmail.com",    "GOLD",   "15/03/2023", "M", 35],
    ["MARÍA",   "LÓPEZ",     "31234567", "1167890123", "maria.lopez@hotmail.com",  "SILVER", "20/04/2023", "F", 28],
    ["pedro",   "martinez",  "35123456", "1198765432", "pedro@email.com",          "GOLD",   "10/01/2023", "M", 42],
    ["Ana",     "Rodríguez", "41789012", "1145678902", "ana.r@gmail.com",          "BASIC",  "05/05/2023", "F", 22],
    ["Carlos",  "Fernández", "29876543", "",           "",                         "GOLD",   "18/02/2023", "M", 51],
    ["Laura",   "González",  "38456123", "1156789012", "laura.g@gmail.com",        "SILVER", "12/06/2023", "F", 31],
    [" Diego ", "Suárez",    "32145678", "1178901234", "diego.suarez@yahoo.com",   "BASIC",  "28/02/2023", "M", 19],
    ["Sofía",   "Torres",    "40234567", "1190123456", "sofia.torres@gmail.com",   "GOLD",   "01/07/2023", "F", 26],
    ["Miguel",  "Ramírez",   "27654321", "1112345678", "miguel.r@gmail.com",       "SILVER", "20/01/2023", "M", 58],
    ["Elena",   "Castro",    "28456789", "1134567890", "elena.c@gmail.com",        "GOLD",   "15/03/2023", "F", 33],  # DNI duplicado
])
cordoba.columns = ["nombre","apellido","dni","telefono","email","plan","fecha_alta","sexo","edad"]
cordoba.to_excel("../data/excels/socios_cordoba_2023.xlsx", index=False)
print("✔ socios_cordoba_2023.xlsx creado")

# ── SUCURSAL ROSARIO ──────────────────────────────────────
rosario = pd.DataFrame([
    ["Roberto",   "Morales",   "31234567", "1167891234", "roberto.m@hotmail.com",   "BASIC",  "2023-03-18", "M", 45],  # DNI dup con Córdoba
    ["Patricia",  "Ruiz",      "39456789", "1189012345", "patricia.ruiz@gmail.com", "GOLD",   "2023-04-25", "F", 38],
    ["Fernando",  "Díaz",      "28901234", "1101234567", "fernando.diaz@gmail.com", "SILVER", "2023-02-10", "M", 29],
    ["Valeria",   "Medina",    "33678901", "1123456789", "",                        "BASIC",  "2023-05-20", "F", 24],
    ["Javier",    "Herrera",   "42123456", "1145678902", "javier.h@gmail.com",      "GOLD",   "2023-06-01", "M", 16],  # menor de edad
    ["CAMILA",    "VARGAS",    "37890123", "1167890124", "camila.v@gmail.com",      "silver", "2023-01-30", "F", 27],
    ["Lucas",     "Jiménez",   "30567890", "1189012346", "lucas.j@hotmail.com",     "BASIC",  "22-03-2023", "M", 34],
    ["Natalia",   "Romero",    "41234567", "1101234568", "natalia.r@gmail.com",     "GOLD",   "2023-04-08", "F", 41],
    ["Andrés",    "Álvarez",   "26901234", "1123456780", "no-es-un-email",          "BASIC",  "2023-05-15", "M", 63],  # email inválido
    ["Florencia", "Gutiérrez", "35678901", "1145678903", "flor.g@gmail.com",        "GOLD",   "20/06/2023", "F", 22],
])
rosario.columns = ["nombre","apellido","dni","telefono","email","plan","fecha_alta","sexo","edad"]
rosario.to_excel("../data/excels/socios_rosario_2023.xlsx", index=False)
print("✔ socios_rosario_2023.xlsx creado")

# ── SUCURSAL BUENOS AIRES ─────────────────────────────────
bsas = pd.DataFrame([
    ["Máximo",    "Reyes",    "28123456", "1167890125", "maximo@gmail.com",      "GOLD",     "01/02/2023", "M", 44],
    ["Isabella",  "Flores",   "39890123", "1189012347", "",                      "PREMIUM",  "30/03/2023", "F", 31],  # plan inválido
    ["Tomás",     "Mendoza",  "32456789", "1101234569", "tomas.m@gmail.com",     "BASIC",    "17/04/2023", "M", 28],
    ["Agustina",  "Cruz",     "40123456", "1123456781", "agustina.c@gmail.com",  "GOLD",     "25/05/2023", "F", 19],
    ["Nicolás",   "Rojas",    "",         "1145678904", "nicolas.r@hotmail.com", "SILVER",   "10/06/2023", "M", 37],  # sin DNI
    ["Luciana",   "Ortega",   "36567890", "1167890126", "luciana.o@gmail.com",   "BASIC",    "08/01/2023", "F", 52],
    ["Gonzalo",   "Vega",     "31234568", "1189012348", "gonzalo.v@gmail.com",   "GOLD",     "07/01/2023", "M", 46],
    [" micaela",  "Ríos",     "41234567", "1101234570", "mica.rios@gmail.com",   "SILVER",   "22/02/2023", "F", 23],  # DNI dup Rosario
    ["Sebastián", "Molina",   "40789012", "1123456782", "seba.molina@gmail.com", "BASIC",    "14/03/2023", "M", 29],
    ["Valentina", "Núñez",    "29456789", "1145678905", "vale.nunez@gmail.com",  "GOLD",     "30/04/2023", "F", 35],
])
bsas.columns = ["nombre","apellido","dni","telefono","email","plan","fecha_alta","sexo","edad"]
bsas.to_excel("../data/excels/socios_bsas_2023.xlsx", index=False)
print("✔ socios_bsas_2023.xlsx creado")

print("\n✔ 3 archivos Excel generados en data/excels/")
print("  Total registros brutos: 30 (con errores intencionales)")
