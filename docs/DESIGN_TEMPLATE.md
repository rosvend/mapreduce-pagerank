# DESIGN.md — PageRank con MapReduce

> Plantilla para la Entrega A. Complétala ANTES de escribir código. Piensa el diseño primero; el código viene después.

**Autor(es):** **Fecha:**

---

## 1. Representación de los datos

¿Cómo representas cada nodo del grafo como un "ítem" que el `mapper` recibe? Describe la estructura exacta (qué es la clave, qué es el valor, qué contiene la adyacencia).

```
# Ejemplo de un ítem de entrada a mapreduce():
# (completa)

```

---

## 2. Esquema clave-valor por fase

### MAP — ¿qué emite el mapper por cada nodo?

Especifica **todos** los tipos de mensajes que emites (hay más de uno).

| Tipo de mensaje | Clave | Valor | Propósito |
| --- | --- | --- | --- |
| Contribución de rank |  |  |  |
| (¿otro tipo?) |  |  |  |

### SHUFFLE — ¿qué queda agrupado por clave?

(Describe qué recibe el reducer para una clave dada.)

### REDUCE — ¿qué retorna el reducer?

Debe retornar la misma estructura que un ítem de entrada, para poder iterar.

---

## 3. Preservación de la estructura del grafo

Explica **cómo** logras que la lista de adyacencia sobreviva de una iteración a la siguiente. ¿Qué pasaría si NO lo hicieras?

---

## 4. Manejo de dangling nodes

¿Qué haces con el rank de un nodo sin enlaces salientes? ¿Por qué? ¿Cómo afecta esto a la invariante de suma (Σ ranks ≈ 1.0)?

---

## 5. Iteración y convergencia

- ¿Dónde vive el bucle de iteración? (fuera de `mapreduce()`)
- Criterio de convergencia (norma L1 < epsilon):
- ¿Cómo comparas los ranks entre iteración N y N+1?

---

## 6. Diagrama de una iteración

```
# Dibuja el flujo: Input -> MAP -> SHUFFLE -> REDUCE -> Output
# (ASCII o adjunta imagen)

```

