export declare class DelimArray<T> extends Array<T> {
    separator: string;
    prefix: string;
    suffix: string;
    constructor(items?: Array<T>, separator?: string, prefix?: string, suffix?: string);
    static create<T>(): DelimArray<T>;
    toString: () => string;
}
