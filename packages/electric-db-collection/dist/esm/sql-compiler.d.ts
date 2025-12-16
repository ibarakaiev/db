import { SubsetParams } from '@electric-sql/client';
import { LoadSubsetOptions } from '@tanstack/db';
export type CompiledSqlRecord = Omit<SubsetParams, `params`> & {
    params?: Array<unknown>;
};
/**
 * Optional encoder function to transform column names from application format
 * to database format (e.g., camelCase to snake_case).
 */
export type ColumnEncoder = (appColumnName: string) => string;
export interface CompileSQLOptions {
    /**
     * Optional encoder function to transform column names.
     * When provided, column names in the generated SQL will be transformed
     * using this function before being quoted.
     */
    encode?: ColumnEncoder;
}
export declare function compileSQL<T>(options: LoadSubsetOptions, compileSQLOptions?: CompileSQLOptions): SubsetParams;
