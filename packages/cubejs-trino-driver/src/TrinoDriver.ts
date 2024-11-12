import {
  DriverInterface,
  StreamOptions,
  StreamTableData,
  BaseDriver,
  DownloadQueryResultsOptions,
  DownloadQueryResultsResult,
} from '@cubejs-backend/base-driver';
import {
  getEnv,
  assertDataSource,
} from '@cubejs-backend/shared';

import SqlString from 'sqlstring';
import { Trino, QueryData } from 'trino-client';
import { PrestodbQuery } from '@cubejs-backend/schema-compiler/dist/src/adapter/PrestodbQuery';
import { Readable } from 'stream';

export type TrinoDriverConfiguration = {
  server?: string;
  catalog?: string;
  schema?: string;
  user?: string;
  password?: string;
  dataSource?: string;
};

export class TrinoDriver extends BaseDriver implements DriverInterface {
  public static getDefaultConcurrency() {
    return 2;
  }

  protected readonly config: TrinoDriverConfiguration;

  protected readonly client: Trino;

  public constructor(config: TrinoDriverConfiguration = {}) {
    super();

    const dataSource = config.dataSource || assertDataSource('default');

    this.config = {
      server: getEnv('dbHost', { dataSource }),
      catalog:
        getEnv('trinoCatalog', { dataSource }) ||
        getEnv('dbCatalog', { dataSource }),
      schema:
        getEnv('dbName', { dataSource }) ||
        getEnv('dbSchema', { dataSource }),
      user: getEnv('dbUser', { dataSource }),
      password: getEnv('dbPass', { dataSource }) || undefined,
      ...config,
    };

    this.client = Trino.create(this.config);
  }

  public async testConnection(): Promise<void> {
    const query = SqlString.format('SHOW SCHEMAS FROM ?', [this.config.catalog]);

    const schemas = await this.query(query, []);
    if (schemas.length === 0) {
      throw new Error(`Catalog not found: '${this.config.catalog}'`);
    }
  }

  public prepareQuery(query: string, values: unknown[]): string {
    return SqlString.format(
      query,
      (values || []).map((value) => (typeof value === 'string'
        ? {
          toSqlString: () => SqlString.escape(value).replace(/\\\\([_%])/g, '\\$1'),
        }
        : value))
    );
  }

  public async query(query: string, values: unknown[]): Promise<any[]> {
    const preparedQuery = this.prepareQuery(query, values);
    const iterator = await this.client.query(preparedQuery);

    const data: any[] = [];

    for await (const result of iterator) {
      if (result.data) {
        data.push(...result.data);
      }
    }

    return data;
  }

  public async stream(
    query: string,
    values: unknown[],
    _options: StreamOptions
  ): Promise<StreamTableData> {
    const preparedQuery = this.prepareQuery(query, values);
    const iterator = await this.client.query(preparedQuery);

    const rowStream = Readable.from(
      await iterator
        .map(r => r.data ?? [])
        .fold<QueryData[]>([], (row, acc) => [...acc, ...row])
    );

    return {
      rowStream,
    };
  }

  public downloadQueryResults(
    query: string,
    values: unknown[],
    options: DownloadQueryResultsOptions
  ): Promise<DownloadQueryResultsResult> {
    if (options.streamImport) {
      return this.stream(query, values, options) as Promise<DownloadQueryResultsResult>;
    }
    return super.downloadQueryResults(query, values, options);
  }

  public static dialectClass() {
    return PrestodbQuery;
  }
}
