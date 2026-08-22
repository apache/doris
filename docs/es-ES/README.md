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

## 🌍 Leer esto en otros idiomas

[English](../../README.md) • [العربية](../ar-SA/README.md) • [বাংলা](../bn-BD/README.md) • [Deutsch](../de-DE/README.md) • [Español](README.md) • [فارسی](../fa-IR/README.md) • [Français](../fr-FR/README.md) • [हिन्दी](../hi-IN/README.md) • [Bahasa Indonesia](../id-ID/README.md) • [Italiano](../it-IT/README.md) • [日本語](../ja-JP/README.md) • [한국어](../ko-KR/README.md) • [Polski](../pl-PL/README.md) • [Português](../pt-BR/README.md) • [Română](../ro-RO/README.md) • [Русский](../ru-RU/README.md) • [Slovenščina](../sl-SI/README.md) • [ไทย](../th-TH/README.md) • [Türkçe](../tr-TR/README.md) • [Українська](../uk-UA/README.md) • [Tiếng Việt](../vi-VN/README.md) • [简体中文](../zh-CN/README.md) • [繁體中文](../zh-TW/README.md)

<div align="center">

# Apache Doris

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![GitHub release](https://img.shields.io/github/release/apache/doris.svg)](https://github.com/apache/doris/releases)
[![Slack](https://img.shields.io/badge/Join%20Our%20Community-Slack-blue)](https://doris.apache.org/slack)
[![EN doc](https://img.shields.io/badge/Docs-English-blue.svg)](https://doris.apache.org/docs/dev/getting-started/what-is-apache-doris)
[![CN doc](https://img.shields.io/badge/文档-中文版-blue.svg)](https://doris.apache.org/zh-CN/docs/dev/getting-started/what-is-apache-doris)

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

<div align="center">
  <a href="https://trendshift.io/repositories/1156" target="_blank"><img src="https://trendshift.io/api/badge/repositories/1156" alt="apache%2Fdoris | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>
</div>

Apache Doris es una base de datos open-source de análisis y búsqueda en tiempo real basada en una arquitectura MPP. Ofrece análisis SQL rápidos, aceleración de consultas lakehouse y búsqueda híbrida sobre datos estructurados, texto y vectores.

Consulta el [sitio web oficial](https://doris.apache.org/) para ver la descripción más reciente del producto, casos de uso, novedades del ecosistema, blogs e historias de usuarios. Para las actualizaciones de versión, consulta [todas las notas de la versión](https://doris.apache.org/releases/all-release).

## 📈 Casos de uso

| Caso de uso | Qué proporciona |
| -------- | ---------------- |
| [Analítica para clientes](https://doris.apache.org/use-cases/customer-facing-analytics) | Ofrece análisis interactivos con latencia inferior al segundo a usuarios externos. |
| [Data warehousing](https://doris.apache.org/use-cases/data-warehousing) | Crea un único almacén en tiempo real para todos los dominios de negocio. |
| [Observabilidad](https://doris.apache.org/use-cases/observability) | Analiza logs, eventos y métricas de alto rendimiento con SQL. |
| [Doris para AI](https://doris.apache.org/use-cases/ai) | Usa búsqueda vectorial, textual, JSON y estructurada en un único motor SQL. |

## 🚀 Capacidades principales

Apache Doris se organiza en torno a tres capacidades principales. El sitio web es la fuente de referencia para las descripciones detalladas del producto y los ejemplos.

| Capacidad | Qué proporciona |
| ---------- | ---------------- |
| [Analítica en tiempo real](https://doris.apache.org/#real-time-analytics) | Ingesta en streaming, transformación incremental y consultas con latencia inferior al segundo con alta concurrencia. |
| [Analítica lakehouse](https://doris.apache.org/#lakehouse-analytics) | Análisis SQL rápidos sobre formatos de tabla abiertos como Iceberg, Delta Lake y Hudi. |
| [Búsqueda híbrida](https://doris.apache.org/#hybrid-search) | Analítica nativa de SQL sobre JSON, texto completo y datos vectoriales para cargas de trabajo de AI y búsqueda. |

## 🔌 Ecosistema

Doris se sitúa en el centro de la pila de datos moderna. Conecta bases de datos de origen, sistemas de streaming y almacenamiento lakehouse con herramientas de BI, AI, analítica y observabilidad.

<p align="center">
  <img src="https://doris.apache.org/images/doris-ecosystem.jpg" alt="Apache Doris ecosystem" width="850" />
</p>

Para ver la cobertura más reciente del ecosistema, visita el [sitio web oficial](https://doris.apache.org/) y la [documentación de conexión e integración](https://doris.apache.org/docs/dev/connection-integration/data-integration/intro).

## 👣 Primeros pasos

- [¿Qué es Apache Doris?](https://doris.apache.org/docs/dev/getting-started/what-is-apache-doris)
- [Inicio rápido](https://doris.apache.org/docs/dev/getting-started/quick-start)
- [Descargar](https://doris.apache.org/download)
- [Instalación](https://doris.apache.org/docs/dev/install/intro)
- [Compilar desde el código fuente](https://doris.apache.org/community/source-install/compilation-with-docker)

## 🧱 Arquitectura

Apache Doris admite despliegues con computación y almacenamiento acoplados, y también despliegues con computación y almacenamiento desacoplados. En el modo desacoplado, los grupos de computación sin estado se ejecutan sobre almacenamiento de objetos compartido, por lo que puedes escalar la computación bajo demanda y aislar cargas de trabajo.

<p align="center">
  <img src="https://doris.apache.org/images/next/home-page/cs-decoupled.jpg" alt="Apache Doris compute-storage decoupled architecture" width="850" />
</p>

Más información en la [guía de despliegue](https://doris.apache.org/docs/dev/install/intro) y la [guía de modos de despliegue](https://doris.apache.org/docs/dev/install/choosing-deployment-mode).

## 📣 Actualizaciones del proyecto

| Recurso | Qué proporciona |
| -------- | ---------------- |
| [Informe de la comunidad](https://doris.apache.org/community-report/) | Actualizaciones semanales sobre actividad de la comunidad, PR fusionadas, contribuidores y avances de funcionalidades. |
| [Roadmap 2026](https://github.com/apache/doris/issues/60036) | Debate de planificación de 2026 para AI y búsqueda híbrida, motor de consultas, almacenamiento y trabajo sobre data lakes. |

## 🧩 Componentes

Doris proporciona conectores y herramientas para flujos de trabajo habituales de ingeniería de datos.

- [Doris Flink Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/flink-doris-connector)
- [Doris Spark Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/spark-doris-connector)
- [Doris Kafka Connector](https://doris.apache.org/docs/dev/connection-integration/data-integration/doris-kafka-connector)
- [Doris Stream Loader](https://doris.apache.org/docs/dev/connection-integration/data-integration/doris-streamloader)
- [Doris Kubernetes Operator](https://doris.apache.org/docs/dev/install/deploy-on-kubernetes/doris-operator/intro)

## 👨‍👩‍👧‍👦 Usuarios

Miles de empresas de todo el mundo usan Apache Doris en producción en servicios de internet, finanzas, retail, logística, fabricación, energía, telecomunicaciones, AI y otros sectores.

- [Usuarios de Apache Doris](https://doris.apache.org/why-doris/users/)
- [Añade tu empresa](https://github.com/apache/doris/discussions/27683)

## 🙌 Colaboradores

Apache Doris se graduó de Apache Incubator y se convirtió en Apache Top-Level Project en junio de 2022. Gracias a todos los [contribuidores de la comunidad](https://github.com/apache/doris/graphs/contributors) que ayudan a construir Doris.

[![contrib graph](https://contrib.rocks/image?repo=apache/doris)](https://github.com/apache/doris/graphs/contributors)

## 🌈 Comunidad y soporte

- [GitHub Issues](https://github.com/apache/doris/issues)
- [GitHub Discussions](https://github.com/apache/doris/discussions)
- [Pull Requests](https://github.com/apache/doris/pulls)
- [Cómo contribuir](https://doris.apache.org/community/how-to-contribute/)
- [Guía de envío de código](https://doris.apache.org/community/how-to-contribute/pull-request/)

## 💬 Contacto

- [Únete a Slack](https://doris.apache.org/slack)
- [Suscríbete a la lista de correo de desarrolladores](https://doris.apache.org/community/subscribe-mail-list)

## 🧰 Enlaces

- Sitio web oficial de Apache Doris: [doris.apache.org](https://doris.apache.org)
- X: [@doris_apache](https://twitter.com/doris_apache)
- Medium: [@ApacheDoris](https://medium.com/@ApacheDoris)

## 📜 Licencia

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)

> **Nota**
> Algunas licencias de dependencias de terceros no son compatibles con Apache License, Version 2.0. Por eso debes desactivar
algunas funcionalidades de Doris para cumplir con Apache License, Version 2.0. Para más detalles, consulta `thirdparty/LICENSE.txt`
