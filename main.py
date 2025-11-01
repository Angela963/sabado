import os
from pathlib import Path

print("\nBienvenido al generador de pipelines para GitHub Actions.\n")

# Versión de Python
language = "python"
version = input("¿Qué versión mínima de Python quieres usar? (Ejemplo: 3.8): ") or "3.8"

# TEST AUTOMATIZADOS
usar_test = input("¿Quieres incluir tests automáticos? (S/n): ") or "s"
steps_test = ''
if usar_test.lower() == 's':
    cmd_test = input("¿Comando para ejecutar tus tests? (por defecto: 'pytest'): ") or "pytest"
    steps_test = f"""
    - name: Ejecutar tests
      run: {cmd_test}"""

# ANALISIS ESTATICO
usar_linter = input("¿Quieres análisis de código estático? (S/n): ") or "s"
steps_linter = ''
linter_pkg = ''
if usar_linter.lower() == 's':
    linter = input("¿Qué herramienta usarás para código estático? ('flake8', 'pylint', etc. por defecto: 'flake8'): ") or "flake8"
    linter_pkg = linter.split()[0]
    cmd_linter = input(f"Comando para análisis estático (por defecto: '{linter} .'): ") or f"{linter} ."
    steps_linter = f"""
    - name: Instalar herramienta de análisis estático
      run: pip install {linter_pkg}
    - name: Análisis de código estático ({linter})
      run: {cmd_linter}"""

# ANALISIS DE SEGURIDAD
usar_security = input("¿Quieres análisis de seguridad? (S/n): ") or "s"
steps_security = ''
security_pkg = ''
if usar_security.lower() == 's':
    security = input("¿Herramienta para análisis de seguridad ('bandit', 'safety', etc. por defecto: 'bandit'): ") or "bandit"
    security_pkg = security.split()[0]
    cmd_security = input(f"Comando para análisis de seguridad (por defecto: '{security} .'): ") or f"{security} ."
    steps_security = f"""
    - name: Instalar herramienta de análisis de seguridad
      run: pip install {security_pkg}
    - name: Análisis de seguridad ({security})
      run: {cmd_security}"""

# Confirmar nombre del archivo
workflow_file = Path(".github/workflows/ci.yml")
workflow_file.parent.mkdir(parents=True, exist_ok=True)

# Plantilla para pipeline completo
yml = f'''name: CI

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - name: Configurar Python {version}
      uses: actions/setup-python@v5
      with:
        python-version: '{version}'
    - name: Instalar dependencias
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt
{steps_linter}{steps_security}{steps_test}
'''

with open(workflow_file, "w", encoding="utf-8") as f:
    f.write(yml)

print(f"\n¡Listo! Archivo de workflow creado en: {workflow_file}\n")
