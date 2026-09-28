/*
 *
 * Copyright 2008-2021 Kinotic and the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {IterablePage} from '@/core/api/crud/IterablePage'
import {Page} from '@/core/api/crud/Page'
import {CursorPageable, OffsetPageable, Pageable} from '@/core/api/crud/Pageable'

export abstract class AbstractIterablePage<T> implements IterablePage<T> {

    private pageable: Pageable
    private currentPage: Page<T>
    private firstPage: boolean = true
    private done: boolean = false
    // Serializes calls to next() so overlapping callers cannot advance the same page twice
    private pending: Promise<unknown> = Promise.resolve()

    protected constructor(pageable: Pageable,
                          page: Page<T>) {
        // Copied so iterating never changes the Pageable the caller passed in
        this.pageable = copyPageable(pageable)
        this.currentPage = page
    }

    /**
     * Finds the next page of results based on the given pageable
     * @param pageable to use to find the next page
     * @return the next page of results
     */
    protected abstract findNext(pageable: Pageable): Promise<Page<T>>;

    next(): Promise<IteratorResult<IterablePage<T>>> {
        const ret = this.pending.then(() => this.advance())
        this.pending = ret.catch(() => undefined)
        return ret
    }

    async return(): Promise<IteratorResult<IterablePage<T>>> {
        this.done = true
        return {done: true, value: undefined}
    }

    [Symbol.asyncIterator](): AsyncIterableIterator<IterablePage<T>> {
        return this
    }

    private async advance(): Promise<IteratorResult<IterablePage<T>>> {
        if(this.done){
            return {done: true, value: undefined}
        }
        if(this.firstPage){
            this.firstPage = false
        }else{
            // Stop without fetching when the page already yielded was the last one
            if(this.isLastPage()){
                this.done = true
                return {done: true, value: undefined}
            }
            const nextPageable = this.nextPageable()
            this.currentPage = await this.findNext(nextPageable)
            // Only advanced once the fetch succeeds, so a failed next() can be retried
            this.pageable = nextPageable
        }
        // An empty page ends iteration, such as querying entities with no results
        // or the trailing empty page a search_after cursor ends with
        if(!this.hasContent()){
            this.done = true
            return {done: true, value: undefined}
        }
        return {done: false, value: new PageSnapshot(this.currentPage, this.isLastPage(), this)}
    }

    private nextPageable(): Pageable {
        const ret = copyPageable(this.pageable)
        if(this.isOffsetPageable()){
            (ret as OffsetPageable).pageNumber++
        }else{
            (ret as CursorPageable).cursor = this.currentPage.cursor as string
        }
        return ret
    }

    hasContent(): boolean {
        return this.currentPage.content !== null && this.currentPage.content !== undefined && this.currentPage.content.length > 0
    }

    isLastPage(): boolean {
        let ret: boolean
        if (this.isOffsetPageable()) {
            const totalElements = this.currentPage.totalElements
            if(totalElements !== null && totalElements !== undefined){
                const numPages = Math.ceil(totalElements / this.pageable.pageSize)
                ret = (this.pageable as OffsetPageable).pageNumber + 1 >= numPages
            }else{
                // Without a total, a short page is the only sign there is nothing more
                ret = (this.currentPage.content?.length ?? 0) < this.pageable.pageSize
            }
        }else{
            // A null cursor means there is no more data
            ret = !this.currentPage.cursor
        }
        return ret
    }

    private isOffsetPageable(): boolean {
        return (this.pageable as OffsetPageable).pageNumber !== undefined
    }

    get totalElements(): number | null | undefined {
        return this.currentPage.totalElements
    }

    get cursor(): string | null | undefined {
        return this.currentPage.cursor
    }

    get content(): T[] | null | undefined {
        return this.currentPage.content
    }

}

function copyPageable(pageable: Pageable): Pageable {
    return Object.assign(Object.create(Object.getPrototypeOf(pageable)), pageable)
}

/**
 * The value yielded for each page, fixed to that page's data so pages collected
 * during iteration keep their own content. Iterating it continues the original iteration.
 */
class PageSnapshot<T> implements IterablePage<T> {

    readonly totalElements: number | null | undefined
    readonly cursor: string | null | undefined
    readonly content: T[] | null | undefined

    constructor(page: Page<T>,
                private readonly lastPage: boolean,
                private readonly iterator: IterablePage<T>) {
        this.totalElements = page.totalElements
        this.cursor = page.cursor
        this.content = page.content
    }

    next(): Promise<IteratorResult<IterablePage<T>>> {
        return this.iterator.next()
    }

    [Symbol.asyncIterator](): AsyncIterableIterator<IterablePage<T>> {
        return this.iterator
    }

    isLastPage(): boolean {
        return this.lastPage
    }

    hasContent(): boolean {
        return this.content !== null && this.content !== undefined && this.content.length > 0
    }

}
