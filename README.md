# AUTOMATIZACIÓN DE DATOS: MIGRACIÓN DE EXCEL A SQL 🚀

![Vista del Proceso](https://github.com/dymadarnas-lgtm/Excel-to-SQL-Automation/blob/main/dashboard_sql-excel.pngraw=true)

Este proyecto valida mi capacidad para migrar procesos operativos antiguos basados en hojas de cálculo hacia infraestructuras de datos modernas y escalables.

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
* `dashboard_sql-excel.png`: Captura de pantalla que muestra la migración exitosa.

---
*Este proyecto demuestra cómo convertir procesos manuales lentos en sistemas automáticos confiables y profesionales.*
