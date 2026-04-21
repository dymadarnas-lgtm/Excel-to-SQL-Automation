# AUTOMATIZACIÓN DE DATOS: MIGRACIÓN DE EXCEL A SQL 🗄️

> **Python · Pandas · SQLAlchemy · MySQL · openpyxl · Scheduler**

---

## 📊 Resultados de la última corrida

| Métrica | Valor |
|---|---|
| 📁 Archivos procesados | **3 Excels** (Córdoba · Rosario · Bs. As.) |
| 👥 Registros totales | **30 socios** analizados |
| ✅ Aprobados y cargados | **21 socios** (70%) |
| ❌ Rechazados con motivo | **9 registros** (30%) |
| ⏱️ Tiempo de ejecución | **< 1 segundo** |
| 🕗 Horas manuales reemplazadas | **8hs/semana → 0hs** |

---

## ⚙️ Pipeline automatizado — 5 pasos

```
[01] Lectura      →  glob escanea la carpeta y lee todos los .xlsx automáticamente
[02] Limpieza     →  normaliza nombres, fechas (3 formatos distintos) y texto
[03] Validación   →  aplica 5 reglas de negocio antes de tocar la base de datos
[04] Carga SQL    →  upsert en MySQL: inserta nuevo o actualiza si el DNI ya existe
[05] Log + Alerta →  genera JSON auditable y muestra resumen en consola
```

---

## 🔴 Motivos de rechazo detectados

```
Email con formato inválido   ████████████████████████  4 casos  (44%)
DNI duplicado entre archivos ██████████████████░░░░░░  3 casos  (33%)
Plan inválido (ej: PREMIUM)  ██████░░░░░░░░░░░░░░░░░░  1 caso   (11%)
DNI vacío                    ██████░░░░░░░░░░░░░░░░░░  1 caso   (11%)
Edad fuera de rango          ██████░░░░░░░░░░░░░░░░░░  1 caso   (11%)
```

---

## 📍 Aprobados por sucursal

```
Buenos Aires   ████████████████████████████░░  8 socios   (80% tasa OK)
Córdoba        █████████████████████████░░░░░  7 socios   (70% tasa OK)
Rosario        ██████████████████████░░░░░░░░  6 socios   (60% tasa OK)
```

---

## ⏱️ Antes vs. después

```
                     ANTES (manual)          DESPUÉS (automatizado)
─────────────────────────────────────────────────────────────────────
Tiempo de proceso    8 horas / semana   →    < 1 segundo
Duplicados           Frecuentes         →    Imposibles (upsert SQL)
Trazabilidad         Ninguna            →    Log JSON por cada corrida
Errores en producción Comunes           →    0 (validación previa)
Reporte              Manual en Excel    →    Automático cada lunes 8am
Horas anuales        416 horas          →    0 horas
```

---

## 🕗 Scheduler — ejecución automática

```python
# Corre solo, cada lunes a las 8:00 AM
schedule.every().monday.at("08:00").do(correr_etl)
```

Sin intervención manual. Sin recordatorios. Sin errores humanos.

---

## EL PROBLEMA (CONTEXTO)

La empresa gestionaba el control de calidad mediante archivos **Excel pesados y propensos a errores manuales**. La falta de una base de datos centralizada impedía realizar consultas rápidas y seguras sobre la "Captación de Socios".

## ¿CÓMO LO RESOLVÍ?

* **PIPELINE AUTOMATIZADO**: Diseñé un script en **Python** que escanea carpetas, procesa múltiples archivos Excel en segundos y los carga en una base de datos **MySQL**.
* **INTEGRIDAD DE DATOS**: Implementé validaciones automáticas mediante **Pandas** para detectar DNIs duplicados y corregir formatos de fecha erróneos antes de la subida.
* **MODULARIDAD**: Estructuré el código con un **Scheduler** para permitir ejecuciones programadas, eliminando la necesidad de intervención manual diaria.

## STACK TECNOLÓGICO

* **PYTHON (PANDAS / SQLALCHEMY)**: Para la lógica de transformación (ETL) y la conexión robusta con la base de datos.
* **MYSQL**: Motor de base de datos utilizado para centralizar la información operativa.
* **OPENPYXL**: Librería clave para la lectura eficiente de archivos `.xlsx` complejos.

## ESTRUCTURA DEL REPO (FILES)

* `etl_pipeline.py`: El corazón del proyecto; realiza la carga y limpieza de datos hacia SQL.
* `generar_excels.py`: Script de utilidad para normalizar los archivos de entrada.
* `scheduler.py`: Componente para programar la automatización de tareas.

---

*Este proyecto demuestra cómo convertir procesos manuales lentos en sistemas automáticos confiables y profesionales.*
