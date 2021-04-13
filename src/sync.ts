// tslint:disable: no-unsafe-any
import { IKeyValueStoreSync } from "@konceiver/kv";
import BetterSqlite3 from "better-sqlite3";
import sql from "sql";

import { DatabaseOptions } from "./interfaces";

export class StoreSync<K, T> implements IKeyValueStoreSync<K, T> {
	private readonly store: BetterSqlite3.Database;
	private readonly opts: DatabaseOptions;
	private readonly table: any;

	private constructor({
		store,
		opts,
		table,
	}: {
		store: BetterSqlite3.Database;
		opts: DatabaseOptions;
		table: any;
	}) {
		this.store = store;
		this.opts = opts;
		this.table = table;
	}

	public static new<K, T>(opts: DatabaseOptions): StoreSync<K, T> {
		// tslint:disable-next-line: no-parameter-reassignment
		opts = {
			keySize: 255,
			table: "ckvs",
			...opts,
		};

		const store = new BetterSqlite3(opts.connection);

		sql.setDialect("sqlite");

		const table = sql.define({
			columns: [
				{
					dataType: `VARCHAR(${Number(opts.keySize)})`,
					// @ts-ignore
					name: "key",
					primaryKey: true,
				},
				{
					// @ts-ignore
					dataType: opts.type,
					// @ts-ignore
					name: "value",
				},
			],
			// @ts-ignore
			name: opts.table,
		});

		store.exec(table.create().ifNotExists().toString());

		return new StoreSync<K, T>({ store, table, opts });
	}

	public all(): [K, T][] {
		return this.store
			.prepare(this.table.select().toString())
			.all()
			.map((row: { key: K; value: T }) => [row.key, row.value]);
	}

	public keys(): K[] {
		return this.store
			.prepare(this.table.select(this.table.key).toString())
			.all()
			.map((row: { key: K }) => row.key);
	}

	public values(): T[] {
		return this.store
			.prepare(this.table.select(this.table.value).toString())
			.all()
			.map((row: { value: T }) => row.value);
	}

	public get(key: K): T | undefined {
		try {
			const { value } = this.store
				.prepare(this.table.select(this.table.value).where({ key }).toString())
				.get();

			return value;
		} catch (error) {
			return undefined;
		}
	}

	public getMany(keys: K[]): (T | undefined)[] {
		return [...keys].map((key: K) => this.get(key));
	}

	public pull(key: K): T | undefined {
		const item: T | undefined = this.get(key);

		this.forget(key);

		return item;
	}

	public pullMany(keys: K[]): (T | undefined)[] {
		const items: (T | undefined)[] = this.getMany(keys);

		this.forgetMany(keys);

		return items;
	}

	public put(key: K, value: T): boolean {
		if (this.has(key)) {
			this.forget(key);
		}

		this.store.exec(this.table.replace({ key, value }).toString());

		return this.has(key);
	}

	public putMany(values: [K, T][]): boolean[] {
		return values.map((value: [K, T]) => this.put(value[0], value[1]));
	}

	public has(key: K): boolean {
		const { exists } = this.store
			.prepare(
				`SELECT EXISTS(SELECT value FROM ${this.opts.table} WHERE key = :key) AS "exists";`
			)
			.get({ key });

		return exists === 1;
	}

	public hasMany(keys: K[]): boolean[] {
		return [...keys].map((key: K) => this.has(key));
	}

	public missing(key: K): boolean {
		return !this.has(key);
	}

	public missingMany(keys: K[]): boolean[] {
		return [...keys].map((key: K) => this.missing(key));
	}

	public forget(key: K): boolean {
		if (!this.has(key)) {
			return false;
		}

		this.store.exec(this.table.delete().where({ key }).toString());

		return this.missing(key);
	}

	public forgetMany(keys: K[]): boolean[] {
		return [...keys].map((key: K) => this.forget(key));
	}

	public flush(): boolean {
		this.store.exec(this.table.delete().toString());

		return this.count() === 0;
	}

	public count(): number {
		const { count } = this.store
			.prepare(this.table.select("COUNT(*) AS count").toString())
			.get();

		return count;
	}

	public isEmpty(): boolean {
		return this.count() === 0;
	}

	public isNotEmpty(): boolean {
		return !this.isEmpty();
	}
}
