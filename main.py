"""
Programa: Alerta Meteorológica Reactiva con RxPy
Tema: PROGRAMACIÓN REACTIVA
Descripción:
- Dos flujos asíncronos: Velocidad del Viento (km/h) e Intensidad de la Lluvia (mm/h)
- Filtrado individual por umbral (Viento>40, Lluvia>8)
- Combinación con verificación de "simultaneidad" en una ventana temporal (p.ej., 2 s)
- Emisión de UNA sola alerta por episodio (evita spam con cooldown/histeresis simple)

Ejecuta este archivo directamente para ver la simulación funcionando.
Luego reemplaza los simuladores por tus fuentes reales (Subjects, sockets, sensores, etc.).
"""

import time
import random
from typing import Dict, Optional, Tuple

import rx
from rx import operators as ops
from rx.scheduler import ThreadPoolScheduler

# ----------------------------
# Parámetros de negocio
# ----------------------------

UMBRAL_VIENTO = 40.0      # km/h
UMBRAL_LLUVIA = 8.0       # mm/h
VENTANA_SEGUNDOS = 2.0    # "al mismo tiempo" = dentro de esta ventana
COOLDOWN_ALERTA = 5.0     # no emitir otra alerta hasta que transcurra este tiempo (anti-spam)

# ----------------------------
# Simuladores de fuentes asíncronas
# ----------------------------
# En un escenario real, reemplaza estos generadores por Subjects u Observables conectados
# a tus sensores/colas/sockets HTTP, etc.

def simulador_viento() -> rx.core.observable.observable.Observable:
    """
    Emite un valor de velocidad de viento cada ~300 ms, con ruido.
    Distribución: la mayoría por debajo del umbral, a veces por encima.
    """
    return rx.interval(0.3).pipe(  # cada 300 ms
        ops.map(lambda i: round(random.uniform(20, 60) + random.choice([0, 0, 0, 10, -5]), 2))
    )

def simulador_lluvia() -> rx.core.observable.observable.Observable:
    """
    Emite un valor de intensidad de lluvia cada ~700 ms, con ruido.
    Distribución: la mayoría por debajo del umbral, a veces por encima.
    """
    return rx.interval(0.7).pipe(  # cada 700 ms
        ops.map(lambda i: round(random.uniform(2, 12) + random.choice([0, 0, 0, 3, -1]), 2))
    )

# ----------------------------
# Utilidades
# ----------------------------

def ahora() -> float:
    """Timestamp en segundos (float)."""
    return time.time()

def evento(nombre: str, valor: float) -> Dict:
    """
    Estandariza un evento con metadatos.
    """
    return {
        "sensor": nombre,    # "viento" | "lluvia"
        "valor": valor,
        "ts": ahora()
    }

def dentro_de_ventana(t1: float, t2: float, ventana: float) -> bool:
    """¿|t1 - t2| <= ventana?"""
    return abs(t1 - t2) <= ventana

# ----------------------------
# Pipeline Reactivo
# ----------------------------

def main():
    # Scheduler opcional (pool de hilos) para paralelizar ligero la simulación
    scheduler = ThreadPoolScheduler(max_workers=4)

    # 1) Fuentes asíncronas (simuladas)
    fuente_viento = simulador_viento().pipe(
        ops.map(lambda v: evento("viento", v))
    )

    fuente_lluvia = simulador_lluvia().pipe(
        ops.map(lambda l: evento("lluvia", l))
    )

    # 2) Filtrado por umbrales (cada flujo por separado)
    viento_peligroso = fuente_viento.pipe(
        ops.filter(lambda e: e["valor"] > UMBRAL_VIENTO),
        # Opcional: suprimir pequeñas oscilaciones cerca del umbral:
        # ops.distinct_until_changed(lambda e: int(e["valor"]))  # discretiza al entero
    )

    lluvia_peligrosa = fuente_lluvia.pipe(
        ops.filter(lambda e: e["valor"] > UMBRAL_LLUVIA),
        # ops.distinct_until_changed(lambda e: int(e["valor"]))
    )

    # 3) Combinación para "simultaneidad"
    #    Estrategia: combine_latest de ambos flujos peligrosos -> cada emisión entrega
    #    el último evento peligroso de viento y de lluvia. Validamos la ventana temporal.
    coincidencias = rx.combine_latest(viento_peligroso, lluvia_peligrosa).pipe(
        ops.map(lambda par: {
            "viento": par[0],   # dict evento viento
            "lluvia": par[1],   # dict evento lluvia
            "ts": ahora()
        }),
        ops.filter(lambda d: dentro_de_ventana(d["viento"]["ts"], d["lluvia"]["ts"], VENTANA_SEGUNDOS)),
    )

    # 4) Anti-spam: emitir UNA sola alerta por episodio (cooldown).
    #    Usamos scan para recordar el último timestamp de alerta emitida.
    #    Si la última alerta es muy reciente, no volvemos a emitir.
    def reductor_alerta(state: Dict, d: Dict) -> Dict:
        """
        state = {"ultimo_alerta_ts": Optional[float], "emitir": bool, "data": Dict}
        """
        ultimo = state.get("ultimo_alerta_ts")
        ahora_ts = d["ts"]
        # ¿Pasó suficiente tiempo desde la última alerta?
        emitir = (ultimo is None) or ((ahora_ts - ultimo) >= COOLDOWN_ALERTA)
        if emitir:
            return {"ultimo_alerta_ts": ahora_ts, "emitir": True, "data": d}
        else:
            return {"ultimo_alerta_ts": ultimo, "emitir": False, "data": d}

    alertas_unicas = coincidencias.pipe(
        ops.scan(reductor_alerta, seed={"ultimo_alerta_ts": None, "emitir": False, "data": None}),
        ops.filter(lambda s: s["emitir"]),  # deja pasar solo cuando toca emitir
        ops.map(lambda s: s["data"])        # nos quedamos con los datos de la coincidencia
    )

    # 5) Suscripciones de salida (logging del panel)
    #    Opcional: ver también los streams crudos/filtrados
    subs = []

    def log_fuente(tag):
        return lambda e: print(f"[{tag}] {e['sensor']:7s}={e['valor']:5.2f}  ts={e['ts']:.3f}")

    def log_alerta(d):
        v, l = d["viento"], d["lluvia"]
        print(
            "\n*** ALERTA METEOROLÓGICA ***\n"
            f"- Viento  > {UMBRAL_VIENTO} km/h : {v['valor']} (ts={v['ts']:.3f})\n"
            f"- Lluvia  > {UMBRAL_LLUVIA} mm/h : {l['valor']} (ts={l['ts']:.3f})\n"
            f"- Δt      = {abs(v['ts'] - l['ts']):.3f} s  (ventana={VENTANA_SEGUNDOS}s)\n"
            f"- Emitida en ts={d['ts']:.3f}\n"
            "*****************************\n"
        )

    # (Opcional) ver todos los datos de entrada
    subs.append(fuente_viento.subscribe(log_fuente("IN   VIENTO"), scheduler=scheduler))
    subs.append(fuente_lluvia.subscribe(log_fuente("IN   LLUVIA"), scheduler=scheduler))

    # (Opcional) ver solo eventos peligrosos individuales
    subs.append(viento_peligroso.subscribe(log_fuente("PELI VTO "), scheduler=scheduler))
    subs.append(lluvia_peligrosa.subscribe(log_fuente("PELI LLV "), scheduler=scheduler))

    # Ver la alerta única combinada
    subs.append(alertas_unicas.subscribe(log_alerta, scheduler=scheduler))

    # 6) Mantener el programa corriendo X segundos para la demo
    #    (en un servicio real, no terminarías el proceso)
    DURACION_DEMO = 30  # segundos
    print(f"\nIniciando demo reactiva durante {DURACION_DEMO} s...\n")
    inicio = ahora()
    try:
        while ahora() - inicio < DURACION_DEMO:
            time.sleep(0.1)
    except KeyboardInterrupt:
        pass
    finally:
        # Cancelar suscripciones limpiamente
        for s in subs:
            s.dispose()
        print("\nDemo finalizada. Suscripciones canceladas.")

if __name__ == "__main__":
    main()
