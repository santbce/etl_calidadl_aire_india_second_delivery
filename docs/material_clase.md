# Relación con el material del tercer corte

Se revisaron los recursos de **Tercer corte** del [curso ETL G51 en CampusDigital](https://campus.uaovirtual.edu.co/course/view.php?id=16085). Los enlaces del campus requieren la sesión del estudiante. Este documento relaciona los conceptos con la implementación propia; las diapositivas y los notebooks del profesor no se redistribuyen en el repositorio.

| Recurso | Concepto aplicado | Implementación del proyecto |
|---|---|---|
| [Event driven architecture — S9 Streaming](https://campus.uaovirtual.edu.co/mod/resource/view.php?id=537969) | Dos rutas de consumo, desacoplamiento, registro persistente y publicación gradual desde la tabla de hechos | El reporte consulta PostgreSQL; el productor publica registros históricos y el consumidor procesa eventos sin consultar el warehouse |
| [Presentación Kafka — S10](https://campus.uaovirtual.edu.co/mod/resource/view.php?id=541204) | Tópico, clave, partición, offset, grupo de consumidores y serialización | `src/final_stream.py`: eventos JSON UTF-8, clave por ciudad, tópico de características y offsets registrados con las predicciones |
| [Práctica Kafka](https://campus.uaovirtual.edu.co/mod/resource/view.php?id=541202) | Broker KRaft, productor y consumidor Python; confirmación de entrega y lectura desde el inicio | Cliente `confluent-kafka`, callbacks de entrega, `flush`, `subscribe`, `poll` y cierre del consumidor; Kafka real en un contenedor local |
| [Repositorio de la práctica](https://github.com/danielfelromero/etl-kafka-blocks-lab) | Separación de bloques de la arquitectura ETL y Kafka | Se conserva el ETL anterior y se añaden productor, consumidor y presentación como componentes explícitos |
| Orquestación y calidad conservadas de la segunda entrega | Dependencias y publicación únicamente tras validación | El DAG final añade entrenamiento, reporte y publicación finita; consumidor y dashboard se ejecutan como servicios separados |
| [Machine learning — S11](https://campus.uaovirtual.edu.co/mod/resource/view.php?id=542127) y [Colab ML](https://campus.uaovirtual.edu.co/mod/resource/view.php?id=542128) | Features y target, separación de datos, `Pipeline`, evaluación y artefactos con `joblib` | `src/final_ml.py`: regresión para AQI del día siguiente, preprocesamiento guardado con el modelo y métricas MAE, RMSE y R² |
| `ml_basico.ipynb` de la práctica | Preprocesamiento ajustado únicamente en entrenamiento; inferencia sin `fit()` | Escalamiento numérico y codificación de ciudad dentro del pipeline; el consumidor carga el pipeline una vez |
| `classification_ml.ipynb` de la práctica | Seleccionar con validación, reentrenar con desarrollo y reservar test | Se adapta a regresión con cortes temporales comunes a las cinco ciudades: entrenamiento hasta 2021, validación 2022 y test 2023–2024 |
| [Github producer](https://github.com/danielfelromero/kafka_producer_etl) | Productor como puente entre datos almacenados y eventos | Productor propio que consulta hechos aprobados y crea mensajes con procedencia, fechas y variables requeridas para inferencia |
| [Aplicaciones en tiempo real — S12](https://campus.uaovirtual.edu.co/mod/resource/view.php?id=542871) | Consumidor de aplicación, inferencia por evento y estado separado de la presentación | Consumidor continuo; SQLite guarda resultados y rechazos; Streamlit presenta ese estado sin consultar la base dimensional |
| [Visualizaciones reactivas](https://campus.uaovirtual.edu.co/mod/resource/view.php?id=542873) | Estado, reruns, caché y fragmentos en Streamlit; callbacks en Dash | `app/dashboard.py`: `st.fragment(run_every='2s')`, filtro por ciudad, contador y evolución de predicciones |

## Adaptaciones justificadas

Las prácticas muestran distintas alternativas, no una única infraestructura obligatoria. Hay ejemplos de Kafka local en Colab, clientes `confluent-kafka` y `kafka-python-ng`, y aplicaciones Streamlit y Dash. El proyecto elige `confluent-kafka` y Streamlit. `compose.yaml` reúne PostgreSQL, Kafka y los procesos Python para una ejecución local reproducible. No se afirma que Docker haya sido exigido en clase.

El notebook de visualización del profesor trabaja con telemetría sintética almacenada en SQLite y aclara que ese ejemplo no implementa Kafka. En esta entrega, SQLite recibe exclusivamente las predicciones y los rechazos del consumidor real; refrescar el dashboard no sustituye el transporte Kafka. Se distingue la actualización periódica de la interfaz del procesamiento por llegada de mensajes.

Los ejemplos introductorios usan particiones aleatorias de datos sintéticos. Aquí la fecha forma parte del problema: los cortes se hacen por fecha objetivo, manteniendo juntas las cinco ciudades en cada periodo. El clima observado del día que se quiere predecir nunca entra en el evento. Las medias móviles usan solamente el día de origen y seis días anteriores.

La práctica de regresión enseña imputación para ilustrar valores faltantes. En este proyecto, el contrato ETL exige datos completos; un dato faltante o no finito detiene la carga o rechaza el evento. No se imputa silenciosamente un error de extracción.

La confirmación de offsets se realiza después de guardar el resultado o rechazo. Si el consumidor se reinicia tras guardar pero antes de confirmar, puede volver a leer el mensaje; la clave única `event_id` evita duplicar la predicción. Esta es una mejora explícita sobre el `auto_commit` de algunos ejemplos didácticos, no una garantía de transacciones distribuidas exactamente una vez.

## Sustentación

La sección Proyecto del campus indica una presentación de 10 a 15 minutos, demostración de datos validados y reporte, y demostración del consumidor con el dashboard actualizándose. También indica que el material audiovisual debe estar totalmente en inglés. El guion propuesto en `docs/entrega_final.md` está en inglés. El bono por socializar en inglés aparece con valores distintos en el PDF de la tarea y el aviso del campus; se debe confirmar con el docente y no asumir puntos adicionales.
