<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

## 🌍 Leer en otros idiomas

[العربية](../ar-SA/README.md) • [বাংলা](../bn-BD/README.md) • [Deutsch](../de-DE/README.md) • [English](../../README.md) • [Español](README.md) • [فارسی](../fa-IR/README.md) • [Français](../fr-FR/README.md) • [हिन्दी](../hi-IN/README.md) • [Bahasa Indonesia](../id-ID/README.md) • [Italiano](../it-IT/README.md) • [日本語](../ja-JP/README.md) • [한국어](../ko-KR/README.md) • [Polski](../pl-PL/README.md) • [Português](../pt-BR/README.md) • [Română](../ro-RO/README.md) • [Русский](../ru-RU/README.md) • [Slovenščina](../sl-SI/README.md) • [ไทย](../th-TH/README.md) • [Türkçe](../tr-TR/README.md) • [Українська](../uk-UA/README.md) • [Tiếng Việt](../vi-VN/README.md) • [简体中文](../zh-CN/README.md) • [繁體中文](../zh-TW/README.md)

<div align="center">

# Apache Doris

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![GitHub release](https://img.shields.io/github/release/apache/doris.svg)](https://github.com/apache/doris/releases)
[![OSSRank](https://shields.io/endpoint?url=https://ossrank.com/shield/516)](https://ossrank.com/p/516)
[![Commit activity](https://img.shields.io/github/commit-activity/m/apache/doris)](https://github.com/apache/doris/commits/master/)
[![EN doc](https://img.shields.io/badge/Docs-English-blue.svg)](https://doris.apache.org/docs/gettingStarted/what-is-apache-doris)
[![CN doc](https://img.shields.io/badge/文档-中文版-blue.svg)](https://doris.apache.org/zh-CN/docs/gettingStarted/what-is-apache-doris)

<div>

[![Official Website](<https://img.shields.io/badge/-Visit%20the%20Official%20Website%20%E2%86%92-rgb(15,214,106)?style=for-the-badge>)](https://doris.apache.org/)
[![Quick Download](<https://img.shields.io/badge/-Quick%20%20Download%20%E2%86%92-rgb(66,56,255)?style=for-the-badge>)](https://doris.apache.org/download)


</div>


<div>
    <a href="https://twitter.com/doris_apache"><img src="https://img.shields.io/badge/- @Doris_Apache -424549?style=social&logo=x" height=25></a>
    &nbsp;
    <a href="https://github.com/apache/doris/discussions"><img src="https://img.shields.io/badge/- Discussion -red?style=social&logo=discourse" height=25></a>
    &nbsp;
    <a href="https://doris.apache.org/slack"><img src="https://img.shields.io/badge/-Slack-4A154B?style=social&logo=slack" height=25 alt="Slack"></a>
    &nbsp;
    <a href="https://medium.com/@ApacheDoris"><img src="https://img.shields.io/badge/-Medium-red?style=social&logo=medium" height=25></a>

</div>

</div>

---

<p align="center">

  <a href="https://trendshift.io/repositories/1156" target="_blank"><img src="https://trendshift.io/api/badge/repositories/1156" alt="apache%2Fdoris | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>

</p>




Apache Doris es una base de datos analítica fácil de usar, de alto rendimiento y en tiempo real basada en la arquitectura MPP, conocida por su velocidad extrema y facilidad de uso. Solo requiere un tiempo de respuesta inferior a un segundo para devolver resultados de consulta bajo datos masivos y puede admitir no solo escenarios de consulta puntual de alta concurrencia sino también escenarios de análisis complejo de alto rendimiento.

Todo esto hace que Apache Doris sea una herramienta ideal para escenarios que incluyen análisis de informes, consultas ad-hoc, almacén de datos unificado y aceleración de consultas de data lake. En Apache Doris, los usuarios pueden construir diversas aplicaciones, como análisis de comportamiento del usuario, plataforma de pruebas AB, análisis de recuperación de registros, análisis de perfil de usuario y análisis de pedidos.

🎉 Consulte 🔗[Todas las versiones](https://doris.apache.org/docs/releasenotes/all-release), donde encontrará un resumen cronológico de las versiones de Apache Doris publicadas durante el año pasado.

👀 Explore el 🔗[Sitio web oficial](https://doris.apache.org/) para descubrir en detalle las características principales, blogs y casos de uso de Apache Doris.

## 📈 Escenarios de uso

Como se muestra en la figura a continuación, después de varias integraciones y procesamientos de datos, las fuentes de datos generalmente se almacenan en el almacén de datos en tiempo real Apache Doris y el data lake o almacén de datos fuera de línea (en Apache Hive, Apache Iceberg o Apache Hudi).

<br />

<img src="https://cdn.selectdb.com/static/What_is_Apache_Doris_3_a61692c2ce.png" />

<br />


Apache Doris se usa ampliamente en los siguientes escenarios:

- **Análisis de datos en tiempo real**:

  - **Informes y toma de decisiones en tiempo real**: Doris proporciona informes y paneles actualizados en tiempo real para uso empresarial interno y externo, apoyando la toma de decisiones en tiempo real en procesos automatizados.
  
  - **Análisis ad-hoc**: Doris ofrece capacidades de análisis de datos multidimensionales, permitiendo análisis rápidos de inteligencia empresarial y consultas ad-hoc para ayudar a los usuarios a descubrir rápidamente información de datos complejos.
  
  - **Perfilado de usuarios y análisis de comportamiento**: Doris puede analizar comportamientos de usuarios como participación, retención y conversión, mientras que también admite escenarios como información demográfica y selección de grupos para análisis de comportamiento.

- **Análisis de data lake**:

  - **Aceleración de consultas de data lake**: Doris acelera las consultas de datos del data lake con su motor de consulta eficiente.
  
  - **Análisis federado**: Doris admite consultas federadas en múltiples fuentes de datos, simplificando la arquitectura y eliminando silos de datos.
  
  - **Procesamiento de datos en tiempo real**: Doris combina capacidades de procesamiento de flujos de datos en tiempo real y por lotes para satisfacer las necesidades de alta concurrencia y baja latencia de requisitos comerciales complejos.

- **Observabilidad basada en SQL**:

  - **Análisis de registros y eventos**: Doris permite análisis en tiempo real o por lotes de registros y eventos en sistemas distribuidos, ayudando a identificar problemas y optimizar el rendimiento.


## Arquitectura general

Apache Doris utiliza el protocolo MySQL, es altamente compatible con la sintaxis MySQL y admite SQL estándar. Los usuarios pueden acceder a Apache Doris a través de varias herramientas cliente y se integra perfectamente con herramientas BI.

### Arquitectura integrada de almacenamiento y computación

La arquitectura integrada de almacenamiento y computación de Apache Doris es simplificada y fácil de mantener. Como se muestra en la figura a continuación, consta de solo dos tipos de procesos:

- **Frontend (FE):** Principalmente responsable de manejar solicitudes de usuarios, análisis y planificación de consultas, gestión de metadatos y tareas de gestión de nodos.

- **Backend (BE):** Principalmente responsable del almacenamiento de datos y la ejecución de consultas. Los datos se particionan en fragmentos y se almacenan con múltiples réplicas en los nodos BE.

![La arquitectura general de Apache Doris](https://cdn.selectdb.com/static/What_is_Apache_Doris_adb26397e2.png)

<br />

En un entorno de producción, se pueden implementar múltiples nodos FE para recuperación ante desastres. Cada nodo FE mantiene una copia completa de los metadatos. Los nodos FE se dividen en tres roles:

| Rol      | Función                                                     |
| --------- | ------------------------------------------------------------ |
| Master    | El nodo FE Master es responsable de las operaciones de lectura y escritura de metadatos. Cuando ocurren cambios de metadatos en el Master, se sincronizan con los nodos Follower u Observer a través del protocolo BDB JE. |
| Follower  | El nodo Follower es responsable de leer metadatos. Si el nodo Master falla, se puede seleccionar un nodo Follower como nuevo Master. |
| Observer  | El nodo Observer es responsable de leer metadatos y se usa principalmente para aumentar la concurrencia de consultas. No participa en las elecciones de liderazgo del clúster. |

Tanto los procesos FE como BE son escalables horizontalmente, permitiendo que un solo clúster admita cientos de máquinas y decenas de petabytes de capacidad de almacenamiento. Los procesos FE y BE utilizan un protocolo de consistencia para garantizar la alta disponibilidad de los servicios y la alta confiabilidad de los datos. La arquitectura integrada de almacenamiento y computación está altamente integrada, reduciendo significativamente la complejidad operativa de los sistemas distribuidos.


## Características principales de Apache Doris

- **Alta disponibilidad**: En Apache Doris, tanto los metadatos como los datos se almacenan con múltiples réplicas, sincronizando los registros de datos a través del protocolo quorum. La escritura de datos se considera exitosa una vez que la mayoría de las réplicas han completado la escritura, asegurando que el clúster permanezca disponible incluso si algunos nodos fallan. Apache Doris admite recuperación ante desastres dentro de la misma ciudad y entre regiones, permitiendo modos maestro-esclavo de doble clúster. Cuando algunos nodos experimentan fallas, el clúster puede aislar automáticamente los nodos defectuosos, evitando que la disponibilidad general del clúster se vea afectada.

- **Alta compatibilidad**: Apache Doris es altamente compatible con el protocolo MySQL y admite sintaxis SQL estándar, cubriendo la mayoría de las funciones MySQL y Hive. Esta alta compatibilidad permite a los usuarios migrar e integrar sin problemas aplicaciones y herramientas existentes. Apache Doris admite el ecosistema MySQL, permitiendo a los usuarios conectar Doris usando herramientas cliente MySQL para operaciones y mantenimiento más convenientes. También admite compatibilidad con el protocolo MySQL para herramientas de informes BI y herramientas de transmisión de datos, asegurando eficiencia y estabilidad en los procesos de análisis de datos y transmisión de datos.

- **Almacén de datos en tiempo real**: Basado en Apache Doris, se puede construir un servicio de almacén de datos en tiempo real. Apache Doris ofrece capacidades de ingesta de datos a nivel de segundo, capturando cambios incrementales de bases de datos transaccionales en línea aguas arriba en Doris en cuestión de segundos. Aprovechando motores vectorizados, arquitectura MPP y motores de ejecución Pipeline, Doris proporciona capacidades de consulta de datos inferiores a un segundo, construyendo así una plataforma de almacén de datos en tiempo real de alto rendimiento y baja latencia.

- **Data lake unificado**: Apache Doris puede construir una arquitectura de data lake unificado basada en fuentes de datos externas como data lakes o bases de datos relacionales. La solución de data lake unificado de Doris permite una integración perfecta y un flujo libre de datos entre data lakes y almacenes de datos, ayudando a los usuarios a utilizar directamente las capacidades del almacén de datos para resolver problemas de análisis de datos en data lakes mientras aprovechan plenamente las capacidades de gestión de datos del data lake para mejorar el valor de los datos.

- **Modelado flexible**: Apache Doris ofrece varios enfoques de modelado, como modelos de tabla ancha, modelos de pre-agregación, esquemas estrella/copo de nieve, etc. Durante la importación de datos, los datos pueden aplanarse en tablas anchas y escribirse en Doris a través de motores de computación como Flink o Spark, o los datos pueden importarse directamente en Doris, realizando operaciones de modelado de datos a través de vistas, vistas materializadas o uniones multi-tabla en tiempo real.

## Resumen técnico

Doris proporciona una interfaz SQL eficiente y es totalmente compatible con el protocolo MySQL. Su motor de consulta se basa en una arquitectura MPP (procesamiento masivamente paralelo), capaz de ejecutar eficientemente consultas analíticas complejas y lograr consultas en tiempo real de baja latencia. A través de la tecnología de almacenamiento en columnas para codificación y compresión de datos, optimiza significativamente el rendimiento de las consultas y la relación de compresión del almacenamiento.

### Interfaz

Apache Doris adopta el protocolo MySQL, admite SQL estándar y es altamente compatible con la sintaxis MySQL. Los usuarios pueden acceder a Apache Doris a través de varias herramientas cliente e integrarlo perfectamente con herramientas BI, incluyendo pero no limitado a Smartbi, DataEase, FineBI, Tableau, Power BI y Apache Superset. Apache Doris puede funcionar como fuente de datos para cualquier herramienta BI que admita el protocolo MySQL.

### Motor de almacenamiento

Apache Doris tiene un motor de almacenamiento en columnas, que codifica, comprime y lee datos por columna. Esto permite una relación de compresión de datos muy alta y reduce en gran medida el escaneo innecesario de datos, haciendo así un uso más eficiente de los recursos IO y CPU.

Apache Doris admite varias estructuras de índice para minimizar los escaneos de datos:

- **Índice de clave compuesta ordenada**: Los usuarios pueden especificar como máximo tres columnas para formar una clave de ordenación compuesta. Esto puede podar efectivamente los datos para apoyar mejor los escenarios de informes altamente concurrentes.

- **Índice Min/Max**: Esto permite un filtrado efectivo de datos en consultas de equivalencia y rango de tipos numéricos.

- **Índice BloomFilter**: Esto es muy efectivo en el filtrado de equivalencia y poda de columnas de alta cardinalidad.

- **Índice invertido**: Esto permite búsquedas rápidas para cualquier campo.

Apache Doris admite una variedad de modelos de datos y los ha optimizado para diferentes escenarios:

- **Modelo de detalle (Modelo de clave duplicada):** Un modelo de datos de detalle diseñado para satisfacer los requisitos de almacenamiento detallado de las tablas de hechos.

- **Modelo de clave primaria (Modelo de clave única):** Asegura claves únicas; los datos con la misma clave se sobrescriben, permitiendo actualizaciones de datos a nivel de fila.

- **Modelo de agregación (Modelo de clave de agregación):** Fusiona columnas de valores con la misma clave, mejorando significativamente el rendimiento a través de la pre-agregación.

Apache Doris también admite vistas materializadas de tabla única fuertemente consistentes y vistas materializadas multi-tabla actualizadas de forma asíncrona. Las vistas materializadas de tabla única se actualizan y mantienen automáticamente por el sistema, sin requerir intervención manual de los usuarios. Las vistas materializadas multi-tabla se pueden actualizar periódicamente usando programación dentro del clúster o herramientas de programación externas, reduciendo la complejidad del modelado de datos.

### 🔍 Motor de consulta

Apache Doris tiene un motor de consulta basado en MPP para ejecución paralela entre y dentro de los nodos. Admite unión shuffle distribuida para tablas grandes para manejar mejor consultas complicadas.

<br />

![Query Engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_1_c6f5ba2af9.png)

<br />

El motor de consulta de Apache Doris está completamente vectorizado, con todas las estructuras de memoria dispuestas en un formato de columnas. Esto puede reducir en gran medida las llamadas de función virtual, aumentar las tasas de acierto de caché y hacer un uso eficiente de las instrucciones SIMD. Apache Doris ofrece un rendimiento 5-10 veces mayor en escenarios de agregación de tabla ancha que los motores no vectorizados.

<br />

![Doris query engine](https://cdn.selectdb.com/static/What_is_Apache_Doris_2_29cf58cc6b.png)

<br />

Apache Doris utiliza tecnología de ejecución de consulta adaptativa para ajustar dinámicamente el plan de ejecución basado en estadísticas de tiempo de ejecución. Por ejemplo, puede generar un filtro de tiempo de ejecución y empujarlo al lado de la sonda. Específicamente, empuja los filtros al nodo de escaneo de nivel más bajo en el lado de la sonda, lo que reduce en gran medida la cantidad de datos a procesar y aumenta el rendimiento de unión. El filtro de tiempo de ejecución de Apache Doris admite In/Min/Max/Bloom Filter.

Apache Doris utiliza un motor de ejecución Pipeline que descompone las consultas en múltiples sub-tareas para ejecución paralela, aprovechando plenamente las capacidades de CPU multi-núcleo. Simultáneamente aborda el problema de explosión de hilos limitando el número de hilos de consulta. El motor de ejecución Pipeline reduce la copia y el intercambio de datos, optimiza las operaciones de ordenación y agregación, mejorando así significativamente la eficiencia y el rendimiento de las consultas.

En términos del optimizador, Apache Doris emplea una estrategia de optimización combinada de CBO (Optimizador basado en costos), RBO (Optimizador basado en reglas) y HBO (Optimizador basado en historial). RBO admite plegado constante, reescritura de subconsultas, empuje de predicados y más. CBO admite reordenamiento de uniones y otras optimizaciones. HBO recomienda el plan de ejecución óptimo basado en información de consulta histórica. Estas múltiples medidas de optimización aseguran que Doris pueda enumerar planes de consulta de alto rendimiento para varios tipos de consultas.


## 🎆 ¿Por qué elegir Apache Doris?

- 🎯 **Fácil de usar**: Dos procesos, sin otras dependencias; escalado de clúster en línea, recuperación automática de réplicas; compatible con el protocolo MySQL y usando SQL estándar.

- 🚀 **Alto rendimiento**: Rendimiento extremadamente rápido para consultas de baja latencia y alto rendimiento con motor de almacenamiento en columnas, arquitectura MPP moderna, motor de consulta vectorizado, vista materializada pre-agregada e índice de datos.

- 🖥️ **Unificado único**: Un solo sistema puede admitir escenarios de servicio de datos en tiempo real, análisis de datos interactivo y procesamiento de datos fuera de línea.

- ⚛️ **Consultas federadas**: Admite consultas federadas de data lakes como Hive, Iceberg, Hudi y bases de datos como MySQL y Elasticsearch.

- ⏩ **Varios métodos de importación de datos**: Admite importación por lotes desde HDFS/S3 e importación de flujo desde MySQL Binlog/Kafka; admite escritura por micro-lotes a través de la interfaz HTTP y escritura en tiempo real usando Insert en JDBC.

- 🚙 **Ecología rica**: Spark usa Spark-Doris-Connector para leer y escribir Doris; Flink-Doris-Connector permite que Flink CDC implemente escritura de datos exactamente una vez en Doris; DBT Doris Adapter se proporciona para transformar datos en Doris con DBT.

## 🙌 Contribuyentes

**Apache Doris se ha graduado exitosamente del incubador Apache y se ha convertido en un proyecto de nivel superior en junio de 2022**.

Apreciamos profundamente a los 🔗[contribuyentes de la comunidad](https://github.com/apache/doris/graphs/contributors) por su contribución a Apache Doris.

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 👨‍👩‍👧‍👦 Usuarios

Apache Doris ahora tiene una amplia base de usuarios en China y en todo el mundo, y hasta la fecha, **Apache Doris se usa en entornos de producción en miles de empresas en todo el mundo.** Más del 80% de las 50 principales empresas de Internet en China en términos de capitalización de mercado o valoración han estado usando Apache Doris durante mucho tiempo, incluyendo Baidu, Meituan, Xiaomi, Jingdong, Bytedance, Tencent, NetEase, Kwai, Sina, 360, Mihoyo y Ke Holdings. También se usa ampliamente en algunas industrias tradicionales como finanzas, energía, manufactura y telecomunicaciones.

Los usuarios de Apache Doris: 🔗[Usuarios](https://doris.apache.org/users)

Agregue el logo de su empresa en el sitio web de Apache Doris: 🔗[Agregar su empresa](https://github.com/apache/doris/discussions/27683)
 
## 👣 Comenzar

### 📚 Documentación

Toda la documentación   🔗[Documentación](https://doris.apache.org/docs/gettingStarted/what-is-apache-doris)  

### ⬇️ Descargar 

Todas las versiones de release y binarias 🔗[Descargar](https://doris.apache.org/download) 

### 🗄️ Compilar

Ver cómo compilar  🔗[Compilación](https://doris.apache.org/community/source-install/compilation-with-docker))

### 📮 Instalar

Ver cómo instalar y desplegar 🔗[Instalación y despliegue](https://doris.apache.org/docs/install/preparation/env-checking) 

## 🧩 Componentes

### 📝 Doris Connector

Doris proporciona soporte para Spark/Flink para leer datos almacenados en Doris a través de Connector, y también admite escribir datos en Doris a través de Connector.

🔗[apache/doris-flink-connector](https://github.com/apache/doris-flink-connector)

🔗[apache/doris-spark-connector](https://github.com/apache/doris-spark-connector)


## 🌈 Comunidad y soporte

### 📤 Suscribirse a listas de correo

La lista de correo es la forma de comunicación más reconocida en la comunidad Apache. Ver cómo 🔗[Suscribirse a listas de correo](https://doris.apache.org/community/subscribe-mail-list)

### 🙋 Reportar problemas o enviar Pull Request

Si tiene alguna pregunta, no dude en presentar un 🔗[GitHub Issue](https://github.com/apache/doris/issues) o publicarlo en 🔗[GitHub Discussion](https://github.com/apache/doris/discussions) y corregirlo enviando un 🔗[Pull Request](https://github.com/apache/doris/pulls) 

### 🍻 Cómo contribuir

Damos la bienvenida a sus sugerencias, comentarios (incluidas críticas), comentarios y contribuciones. Ver 🔗[Cómo contribuir](https://doris.apache.org/community/how-to-contribute/) y 🔗[Guía de envío de código](https://doris.apache.org/community/how-to-contribute/pull-request/)

### ⌨️ Propuestas de mejora de Doris (DSIP)

🔗[Propuesta de mejora de Doris (DSIP)](https://cwiki.apache.org/confluence/display/DORIS/Doris+Improvement+Proposals) puede considerarse como **Una colección de documentos de diseño para todas las actualizaciones o mejoras importantes de características**.

### 🔑 Especificación de codificación Backend C++
🔗 [Especificación de codificación Backend C++](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=240883637) debe seguirse estrictamente, lo que nos ayudará a lograr una mejor calidad de código.

## 💬 Contáctenos

Contáctenos a través de la siguiente lista de correo.

| Nombre                                                                          | Alcance                           |                                                                 |                                                                     |                                                                              |
|:------------------------------------------------------------------------------|:--------------------------------|:----------------------------------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------------------|
| [dev@doris.apache.org](mailto:dev@doris.apache.org)     | Discusiones relacionadas con el desarrollo | [Suscribirse](mailto:dev-subscribe@doris.apache.org)   | [Cancelar suscripción](mailto:dev-unsubscribe@doris.apache.org)   | [Archivos](http://mail-archives.apache.org/mod_mbox/doris-dev/)   |

## 🧰 Enlaces

* Sitio web oficial de Apache Doris - [Sitio](https://doris.apache.org)
* Lista de correo de desarrolladores - <dev@doris.apache.org>. Envíe un correo a <dev-subscribe@doris.apache.org>, siga la respuesta para suscribirse a la lista de correo.
* Canal de Slack - [Unirse a Slack](https://doris.apache.org/slack)
* Twitter - [Seguir @doris_apache](https://twitter.com/doris_apache)


## 📜 Licencia

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **Nota**
> Algunas licencias de las dependencias de terceros no son compatibles con la licencia Apache 2.0. Por lo tanto, debe desactivar
algunas funciones de Doris para cumplir con la licencia Apache 2.0. Para más detalles, consulte el archivo `thirdparty/LICENSE.txt`


