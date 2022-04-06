# Vodyani kafka-node-adapter

*kafka-node-adapter*

[![Npm](https://img.shields.io/npm/v/@vodyani/kafka-node-adapter)](https://www.npmjs.com/package/@vodyani/kafka-node-adapter)
[![Npm](https://img.shields.io/npm/dm/@vodyani/kafka-node-adapter)](https://www.npmjs.com/package/@vodyani/kafka-node-adapter)
[![License](https://img.shields.io/github/license/vodyani/kafka-node-adapter)](LICENSE)
![Workflow](https://github.com/vodyani/kafka-node-adapter/actions/workflows/release.yml/badge.svg)
[![Known Vulnerabilities](https://snyk.io/test/github/vodyani/kafka-node-adapter/badge.svg?targetFile=package.json)](https://snyk.io/test/github/vodyani/kafka-node-adapter?targetFile=package.json)

## Installation

```sh
npm install @vodyani/kafka-node-adapter
```

## Recommendations

- typescript `v4.6.x`
- node.js `v12.x` ~ `v16.x`

## Dependencies

- [kafka-node](https://github.com/SOHU-Co/kafka-node) `v5`

### Usage

> Due to naming conflicts, you need to introduce additional declare `TypeSequelize` when using `sequelize-typescript`.

```
import { TypeSequelize } from '@vodyani/kafka-node-adapter';
```

## Contributing

## License

Vodyani kafka-node-adapter is [MIT licensed](LICENSE).
