/*
 * Copyright 2015-2020 Imply Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Duration, Timezone } from 'chronoshift';

import { PlyType } from '../types';

import { SQLDialect } from './baseDialect';

export class TrinoDialect extends SQLDialect {
  static TIME_BUCKETING: Record<string, string> = {
    PT1S: 'second',
    PT1M: 'minute',
    PT1H: 'hour',
    P1D: 'day',
    P1W: 'week',
    P1M: 'month',
    P3M: 'quarter',
    P1Y: 'year',
  };

  static TIME_PART_TO_FUNCTION: Record<string, string> = {
    SECOND_OF_MINUTE: 'SECOND($$)',
    SECOND_OF_HOUR: '(MINUTE($$) * 60 + SECOND($$))',
    SECOND_OF_DAY: '((HOUR($$) * 60 + MINUTE($$)) * 60 + SECOND($$))',
    SECOND_OF_WEEK:
      '((((CAST((DAY_OF_WEEK($$) + 6) AS integer) % 7) * 24) + HOUR($$) * 60 + MINUTE($$)) * 60 + SECOND($$))',
    SECOND_OF_MONTH: '((((DAY($$) - 1) * 24) + HOUR($$) * 60 + MINUTE($$)) * 60 + SECOND($$))',
    SECOND_OF_YEAR:
      '((((DAY_OF_YEAR($$) - 1) * 24) + HOUR($$) * 60 + MINUTE($$)) * 60 + SECOND($$))',

    MINUTE_OF_HOUR: 'MINUTE($$)',
    MINUTE_OF_DAY: 'HOUR($$) * 60 + MINUTE($$)',
    MINUTE_OF_WEEK:
      '((CAST((DAY_OF_WEEK($$) + 6) AS integer) % 7) * 24 + HOUR($$) * 60 + MINUTE($$))',
    MINUTE_OF_MONTH: '((DAY($$) - 1) * 24 + HOUR($$) * 60 + MINUTE($$))',
    MINUTE_OF_YEAR: '((DAY_OF_YEAR($$) - 1) * 24 + HOUR($$) * 60 + MINUTE($$))',

    HOUR_OF_DAY: 'HOUR($$)',
    HOUR_OF_WEEK: '((CAST((DAY_OF_WEEK($$) + 6) AS integer) % 7) * 24 + HOUR($$))',
    HOUR_OF_MONTH: '((DAY($$) - 1) * 24 + HOUR($$))',
    HOUR_OF_YEAR: '((DAY_OF_YEAR($$) - 1) * 24 + HOUR($$))',

    DAY_OF_WEEK: '((CAST((DAY_OF_WEEK($$) + 6) AS integer) % 7) + 1)',
    DAY_OF_MONTH: 'DAY($$)',
    DAY_OF_YEAR: 'DAY_OF_YEAR($$)',

    WEEK_OF_YEAR: 'WEEK($$)',

    MONTH_OF_YEAR: 'MONTH($$)',
    YEAR: 'YEAR($$)',
  };

  static CAST_TO_FUNCTION: Record<string, Record<string, string>> = {
    TIME: {
      NUMBER: 'from_unixtime($$ / 1000)',
    },
    NUMBER: {
      TIME: 'cast(extract(epoch from $$) * 1000 as bigint)',
      STRING: '$$::float',
    },
    STRING: {
      NUMBER: '$$::text',
    },
  };

  constructor() {
    super();
  }

  public emptyGroupBy(): string {
    return "GROUP BY ''";
  }

  public timeToSQL(date: Date): string {
    if (!date) return this.nullConstant();
    return `timestamp '${this.dateToSQLDateString(date)}'`;
  }

  public stringArrayToSQL(_value: string[]): string {
    throw new Error('must implement');
  }

  public concatExpression(a: string, b: string): string {
    return `(${a} || ${b})`;
  }

  public containsExpression(a: string, b: string, insensitive: boolean): string {
    if (insensitive) {
      a = `lower(${a})`;
      b = `lower(${b})`;
    }
    return `strpos(${a}, ${b}) > 0`;
  }

  public regexpExpression(expression: string, regexp: string): string {
    return `(${expression} ~ '${regexp}')`; // ToDo: escape this.regexp
  }

  public castExpression(inputType: PlyType, operand: string, targetType: string): string {
    const castFunction = TrinoDialect.CAST_TO_FUNCTION[targetType][inputType];
    if (!castFunction) {
      throw new Error(`unsupported cast from ${inputType} to ${targetType} in Trino dialect`);
    }
    return castFunction.replace(/\$\$/g, operand);
  }

  public utcToWalltime(operand: string, timezone: Timezone): string {
    if (timezone.isUTC()) return operand;
    return `from_utc_timestamp(${operand}, '${timezone}')`;
  }

  public walltimeToUTC(operand: string, timezone: Timezone): string {
    if (timezone.isUTC()) return operand;
    return `to_utc_timestamp(${operand}, '${timezone}')`;
  }

  public timeFloorExpression(operand: string, duration: Duration, timezone: Timezone): string {
    const bucketFormat = TrinoDialect.TIME_BUCKETING[duration.toString()];
    if (!bucketFormat) throw new Error(`unsupported duration '${duration}'`);
    return `date_trunc('${bucketFormat}', ${this.utcToWalltime(operand, timezone)})`;
  }

  public timeBucketExpression(operand: string, duration: Duration, timezone: Timezone): string {
    return this.timeFloorExpression(operand, duration, timezone);
  }

  public timePartExpression(operand: string, part: string, timezone: Timezone): string {
    const timePartFunction = TrinoDialect.TIME_PART_TO_FUNCTION[part];
    if (!timePartFunction) throw new Error(`unsupported part ${part} in Trino dialect`);
    return timePartFunction.replace(/\$\$/g, this.utcToWalltime(operand, timezone));
  }

  public timeShiftExpression(
    operand: string,
    duration: Duration,
    step: number,
    timezone: Timezone,
  ): string {
    if (step === 0) return operand;

    // Implementing date_add and date_sub logic in Trino SQL
    const sqlFn = step > 0 ? 'date_add' : 'date_sub';
    const spans = duration.multiply(Math.abs(step)).valueOf();

    if (spans.week) {
      return `${sqlFn}(${operand}, ${spans.week}, 'week')`;
    }
    if (spans.year || spans.month || spans.day || spans.hour || spans.minute || spans.second) {
      const exprParts = [];
      if (spans.year) exprParts.push(`${spans.year} year`);
      if (spans.month) exprParts.push(`${spans.month} month`);
      if (spans.day) exprParts.push(`${spans.day} day`);
      if (spans.hour) exprParts.push(`${spans.hour} hour`);
      if (spans.minute) exprParts.push(`${spans.minute} minute`);
      if (spans.second) exprParts.push(`${spans.second} second`);

      const intervalExpr = exprParts.join(', ');
      return `${sqlFn}(${operand}, interval '${intervalExpr}')`;
    }

    throw new Error(`unsupported duration '${duration}' for timeShiftExpression in Trino dialect`);
  }

  public extractExpression(operand: string, regexp: string): string {
    return `(SELECT regexp_extract(${operand}, '${regexp}', 1))`;
  }

  public indexOfExpression(str: string, substr: string): string {
    return `strpos(${str}, ${substr}) - 1`;
  }
}
