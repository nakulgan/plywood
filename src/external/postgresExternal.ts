/* [object Object]*/

import { PlywoodRequester } from 'plywood-base-api';
import * as toArray from 'stream-to-array';

import { AttributeInfo, Attributes } from '../datatypes/attributeInfo';
import { PseudoDatum } from '../datatypes/dataset';
import { TrinoDialect } from '../dialect/trinoDialect';
import { PlyType } from '../types';

import { External, ExternalJS, ExternalValue } from './baseExternal';
import { SQLExternal } from './sqlExternal';

export interface TrinoSQLDescribeRow {
  name: string;
  sqlType: string;
  arrayType?: string;
}

export class TrinoExternal extends SQLExternal {
  static engine = 'trino'; // Adjusted to Trino
  static type = 'DATASET';

  static fromJS(parameters: ExternalJS, requester: PlywoodRequester<any>): TrinoExternal {
    const value: ExternalValue = External.jsToValue(parameters, requester);
    return new TrinoExternal(value);
  }

  static postProcessIntrospect(columns: TrinoSQLDescribeRow[]): Attributes {
    return columns
      .map((column: TrinoSQLDescribeRow) => {
        const name = column.name;
        let type: PlyType;
        let nativeType = column.sqlType.toLowerCase();
        if (nativeType.indexOf('timestamp') !== -1) {
          type = 'TIME';
        } else if (nativeType === 'varchar' || nativeType === 'char') {
          // Adjusted for Trino
          type = 'STRING';
        } else if (nativeType === 'integer' || nativeType === 'bigint') {
          // ToDo: make something special for integers
          type = 'NUMBER';
        } else if (nativeType === 'double' || nativeType === 'real') {
          // Adjusted for Trino
          type = 'NUMBER';
        } else if (nativeType === 'boolean') {
          type = 'BOOLEAN';
        } else if (nativeType === 'array') {
          nativeType = column.arrayType.toLowerCase();
          if (nativeType === 'varchar' || nativeType === 'char') {
            type = 'SET/STRING'; // Adjusted for Trino
          } else if (nativeType === 'timestamp') {
            type = 'SET/TIME';
          } else if (
            nativeType === 'integer' ||
            nativeType === 'bigint' ||
            nativeType === 'double' ||
            nativeType === 'real'
          ) {
            type = 'SET/NUMBER';
          } else if (nativeType === 'boolean') {
            type = 'SET/BOOLEAN';
          } else {
            return null;
          }
        } else {
          return null;
        }

        return new AttributeInfo({
          name,
          type,
          nativeType,
        });
      })
      .filter(Boolean);
  }

  static getSourceList(requester: PlywoodRequester<any>): Promise<string[]> {
    return toArray(
      requester({
        query: `SHOW TABLES FROM schemaName`, // Adjust with Trino equivalent for listing tables
      }),
    ).then(sources => {
      if (!sources.length) return sources;
      return sources.map((s: PseudoDatum) => s['tab']).sort();
    });
  }

  static getVersion(requester: PlywoodRequester<any>): Promise<string> {
    return toArray(requester({ query: 'SELECT VERSION()' })).then(res => {
      if (!Array.isArray(res) || res.length !== 1) throw new Error('invalid version response');
      const key = Object.keys(res[0])[0];
      if (!key) throw new Error('invalid version response (no key)');
      let versionString = res[0][key];
      let match: RegExpMatchArray | null;
      if ((match = versionString.match(/^Trino (\S+)/))) versionString = match[1];
      return versionString;
    });
  }

  constructor(parameters: ExternalValue) {
    super(parameters, new TrinoDialect()); // Assuming TrinoDialect is implemented similarly
    this._ensureEngine('trino');
  }

  protected getIntrospectAttributes(): Promise<Attributes> {
    return toArray(
      this.requester({
        query: `DESCRIBE ${this.dialect.escapeLiteral(this.source as string)}`, // Adjust for Trino
      }),
    ).then(TrinoExternal.postProcessIntrospect);
  }
}

External.register(TrinoExternal);
