# Tarea: PageRank con MapReduce (Python puro)

- **Módulo:** modules/01-mapreduce/01-pure-python/01-basics
- **Framework:** `mapreduce_framework.py` (el mismo de los labs) — **sin mrjob, sin librerías externas**
- **Modalidad:** Individual o en parejas
- **Peso:** Componente de evaluación del módulo MapReduce

---

## 1. Contexto

PageRank es el algoritmo que hizo a Google. Asigna a cada página web un "puntaje de importancia" según cuántas páginas la enlazan y qué tan importantes son esas páginas. Es **iterativo**: el puntaje se recalcula una y otra vez hasta que se estabiliza (converge).

Esto lo hace el ejemplo perfecto para cerrar el módulo: **MapReduce fue diseñado para una pasada Map→Shuffle→Reduce, pero PageRank necesita muchas pasadas**. Vas a sentir en carne propia por qué iterar en MapReduce es costoso — exactamente la limitación que motiva a Spark (Clase 5).

> **Nota sobre uso de IA:** Está permitido usar asistentes de IA (Copilot, ChatGPT, Claude, etc.). Esta tarea está **diseñada para que la IA no la resuelva por ti**: el código es solo el 30% de la nota. El resto evalúa tu razonamiento de diseño, tu análisis de costo, tus casos de prueba y tu capacidad de **defender oralmente** cada decisión. Si generas código con IA y no lo entiendes, la defensa oral lo va a evidenciar. Ver §7.

---

## 2. El algoritmo (lo que necesitas saber)

Cada nodo (página) tiene un rank. En cada iteración, cada página **reparte su rank en partes iguales** entre las páginas a las que enlaza. El nuevo rank de una página es la suma de lo que recibe, ajustado por el *damping factor*:

```
rank_nuevo(P) = (1 - d) / N  +  d * Σ [ rank(Q) / outlinks(Q) ]
                                    para cada Q que enlaza a P
```

- `N` = número total de nodos
- `d` = damping factor (usa **0.85**)
- La suma recorre todas las páginas `Q` que enlazan a `P`

Se itera hasta **converger** (los ranks casi no cambian entre iteración e iteración) o hasta un máximo de iteraciones.

---

## 3. El reto de diseño (léelo con atención)

El framework `mapreduce(data, mapper, reducer)` hace **una sola pasada** y el `mapper` recibe **un ítem a la vez**. Esto te obliga a resolver tres problemas no triviales:

1. **Iterar** → tienes que **envolver `mapreduce()` en un bucle Python**. La salida de una iteración es la entrada de la siguiente. El bucle vive en tu código, no en el framework.

2. **Preservar la estructura del grafo** → el `mapper` de PageRank emite las *contribuciones de rank* hacia los vecinos. Pero si solo emites eso, **pierdes quién-enlaza-a-quién** para la siguiente iteración. Tienes que diseñar cómo hacer que la lista de adyacencia **sobreviva** cada pasada. (Pista: el mapper puede emitir dos tipos de mensajes hacia cada clave.)

3. **Nodos colgantes (*dangling nodes*)** → páginas sin enlaces salientes. Su rank "se fuga" del sistema si no lo manejas. Decide qué haces con él y **justifícalo**.

> Ninguna de estas tres decisiones es obvia, y ninguna se resuelve con un prompt de una línea. Ese es el punto.

---

## 4. Requisitos técnicos

- Usar **exclusivamente** `mapreduce_framework.py` (la función `mapreduce()` tal cual). No puedes modificar el framework.
- Python estándar únicamente. Sin `networkx`, sin `numpy`, sin nada externo.
- El `mapper` y el `reducer` deben respetar las firmas del framework:
  - `mapper(item) -> yield (key, value)`
  - `reducer(key, values) -> valor_reducido`
- La lógica de **iteración y convergencia** va en una función aparte (ej. `run_pagerank(graph, d=0.85, max_iter=50, epsilon=1e-6)`) que llama a `mapreduce()` en un bucle.
- **Criterio de convergencia:** detener cuando la suma de las diferencias absolutas de rank entre dos iteraciones (norma L1) sea menor a `epsilon`. Esta comprobación va en tu bucle, **fuera** del `mapreduce()`.

---

## 5. Entregables

Se entregan **en dos momentos**:

### Entrega A — Documento de diseño (ANTES de escribir código)
`DESIGN.md` (usa la plantilla provista). Debe contener:
1. **Esquema clave-valor de cada fase**: qué emite el mapper, qué agrupa el shuffle, qué retorna el reducer. Especifica los *tipos* de mensajes.
2. **Cómo preservas la estructura del grafo** entre iteraciones.
3. **Cómo manejas los dangling nodes** y por qué.
4. **Diagrama del flujo de una iteración** (ASCII o imagen).

> La Entrega A se revisa y se te da feedback **antes** de que programes. No se acepta código sin diseño previo aprobado.

### Entrega B — Implementación + análisis
1. `pagerank.py` — tu implementación completa y ejecutable.
2. `test_pagerank.py` — tus casos de prueba (ver §6).
3. `ANALYSIS.md` — análisis de costo y correctitud (ver §6).
4. `AI_LOG.md` — bitácora de uso de IA (ver §7).

---

## 6. Componentes de razonamiento (lo que más pesa)

### 6.1 Casos de prueba diseñados por ti (`test_pagerank.py`)
Diseña y justifica pruebas para al menos:
- Un **grafo trivial** con resultado calculable a mano (ej. 3 nodos en cadena).
- Un **ciclo** (A→B→C→A): todos los ranks deben ser iguales por simetría.
- Un **dangling node**.
- La **invariante de suma**: la suma de todos los ranks debe mantenerse ≈ 1.0 en cada iteración. Si no se mantiene, tu manejo de dangling nodes está mal.

### 6.2 Análisis de costo (`ANALYSIS.md`)
Responde con argumentos, no con código:
- ¿Cuántos pares (clave, valor) emite el mapper por iteración, en función de aristas `E` y nodos `N`? ¿Cuál es el volumen de *shuffle*?
- Si este framework tuviera **combiners** (como Hadoop/mrjob), ¿dónde pondrías uno y qué pre-agregaría? ¿Por qué reduciría el shuffle?
- ¿Qué pasa si un nodo tiene un *in-degree* gigantesco (data skew)? ¿Qué reducer se vuelve el cuello de botella?
- **Conexión con Clase 5:** estima cuántas veces se "releen" los datos si corres 30 iteraciones. Explica por qué Spark (datos en memoria) sería más rápido aquí.

---

## 7. Bitácora de uso de IA (`AI_LOG.md`) + Defensa oral

### 7.1 Bitácora
Documenta honestamente:
- Qué le pediste a la IA (los prompts relevantes).
- Qué generó que estaba **mal o incompleto** (ej. no preservaba el grafo, o ignoraba dangling nodes).
- Qué **corregiste tú** y por qué.

> Una bitácora que diga "le pedí PageRank y funcionó a la primera" es una señal de alerta: significa que probablemente no entendiste el reto de diseño. Los casos triviales de PageRank que la IA genera casi nunca manejan bien la preservación del grafo ni los dangling nodes sobre un framework single-pass como este.

### 7.2 Defensa oral (5 min)
En la sustentación se te pedirá explicar **una** decisión de diseño elegida al azar (ej. "muéstrame dónde preservas la adyacencia y qué pasa si lo quitas"). Esta es la prueba definitiva de autoría.

---

## 8. Rúbrica

| Componente | Peso | Qué se evalúa |
|------------|------|---------------|
| **Documento de diseño (`DESIGN.md`)** | 25% | Esquema clave-valor correcto, estrategia de preservación del grafo, manejo de dangling nodes, claridad del flujo |
| **Implementación (`pagerank.py`)** | 30% | Corre correctamente, usa el framework sin modificarlo, itera bien, damping y convergencia correctos |
| **Análisis de costo (`ANALYSIS.md`)** | 15% | Volumen de shuffle, ubicación de combiner, skew, conexión con Clase 5 |
| **Casos de prueba (`test_pagerank.py`)** | 10% | Cobertura de edge cases, invariante de suma, resultados verificables a mano |
| **Bitácora de IA (`AI_LOG.md`)** | 10% | Honestidad y profundidad de la reflexión sobre qué falló y qué corrigieron |
| **Defensa oral** | 10% | Capacidad de explicar y justificar decisiones de diseño |

> **El código es solo el 30%.** Un entregable generado con IA sin comprensión puede pasar el 30% de implementación y reprobar el 70% restante.

---

## 9. Dataset

Se proveen **tres** grafos, en el mismo formato (una línea por nodo):

```
nodo: vecino1 vecino2 vecino3
```

Un nodo sin vecinos (línea `E:` sin nada a la derecha) es un **dangling node** — tu implementación debe manejarlo.

| Archivo | Nodos | Aristas | Dangling | Uso |
|---------|-------|---------|----------|-----|
| `web_graph_sample.txt` | 8 | ~11 | 1 | Desarrollo a mano — resultados verificables con lápiz |
| `web_graph_medium.txt` | 1.000 | ~6.100 | 30 | Pruebas rápidas durante el desarrollo |
| `web_graph_large.txt` | 10.000 | ~63.000 | 300 | **Dataset oficial de la entrega** |

Los grafos medium y large se generaron con **preferential attachment**: unos pocos nodos acumulan muchos inlinks (hubs), como en la web real. Esto hace que el ranking sea interesante — los hubs deben emerger en el top de tu PageRank, y el *data skew* de §6.2 es real y medible (los hubs más enlazados tienen ~55-60 inlinks vs. el promedio de ~6).

**Sobre la entrega:** tu análisis de costo (§6.2) y tus resultados deben correr sobre `web_graph_large.txt`. Reporta el **top-15** de páginas por PageRank y contrasta el rank contra el in-degree de cada una (deberían correlacionar, pero no perfectamente — explica por qué no es correlación perfecta).

**Escala de referencia:** sobre `web_graph_large.txt` el algoritmo converge en ~24 iteraciones. Cada iteración mueve ~73.000 pares (clave, valor) en el shuffle (10.000 mensajes STRUCT + ~63.000 mensajes RANK). En 24 iteraciones son ~1.75 millones de pares — un número concreto para tu análisis de §6.2.

**Stretch (opcional, +bonus):** mide el tiempo por iteración sobre el grafo grande y grafícalo. Estima cuánto tardaría con 10x o 100x más nodos, y conéctalo con tu análisis de §6.2 y la lección de Spark (Clase 5): en MapReduce el grafo completo se relee del disco en cada iteración; en Spark viviría en memoria.

---

## 10. Entrega

- **Entrega A (`DESIGN.md`):** sábado 29 de agosto, 11:59 pm (antes de la siguiente clase)
- **Entrega B (código + análisis + bitácora):** sábado 5 de septiembre, 11ß:59 pm
- **Defensa oral:** lunes 7 de septiembre, en horario de clase
- Formato: carpeta en tu repositorio del curso, o zip con todos los archivos.

---

## 11. Integridad académica

El uso de IA está **permitido y es esperado** — es una herramienta profesional. Lo que se evalúa es tu **comprensión**, no tu capacidad de teclear. Presentar trabajo que no puedes explicar es deshonestidad académica. La defensa oral es la salvaguarda.
