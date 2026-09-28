import {describe, expect, it} from 'vitest'
import {FunctionalIterablePage, IterablePage, Page, Pageable} from '../src'

/**
 * Builds an iterable page over a scripted server, recording the pageable of every fetch.
 */
function scripted<T>(pageable: Pageable, first: Page<T>, rest: Page<T>[]) {
    const fetches: Pageable[] = []
    const remaining = [...rest]
    const iterable = new FunctionalIterablePage<T>(pageable, first, async (p) => {
        fetches.push({...p})
        const next = remaining.shift()
        if(!next){
            throw new Error('Fetched past the last scripted page')
        }
        return next
    })
    return {iterable, fetches}
}

async function contents<T>(iterable: IterablePage<T>): Promise<T[][]> {
    const ret: T[][] = []
    for await (const page of iterable) {
        ret.push(page.content as T[])
    }
    return ret
}

describe('IterablePage', () => {

    it('yields the last cursor page when it has content and a null cursor', async () => {
        // Elasticsearch SQL, used by named queries, ends this way
        const {iterable, fetches} = scripted(Pageable.createWithCursor(null, 2),
                                             {content: [1, 2], cursor: 'c1', totalElements: null},
                                             [{content: [3], cursor: null, totalElements: null}])

        expect(await contents(iterable)).toEqual([[1, 2], [3]])
        expect(fetches.map(p => (p as any).cursor)).toEqual(['c1'])
    })

    it('stops at a trailing empty cursor page', async () => {
        // search_after, used by findAll and search, ends this way
        const {iterable} = scripted(Pageable.createWithCursor(null, 2),
                                    {content: [1, 2], cursor: 'c1', totalElements: null},
                                    [{content: [3], cursor: 'c2', totalElements: null},
                                     {content: [], cursor: null, totalElements: null}])

        expect(await contents(iterable)).toEqual([[1, 2], [3]])
    })

    it('does not fetch again after a single cursor page', async () => {
        const {iterable, fetches} = scripted(Pageable.createWithCursor(null, 2),
                                             {content: [1], cursor: null, totalElements: null},
                                             [])

        expect(iterable.isLastPage()).toBe(true)
        expect(await contents(iterable)).toEqual([[1]])
        expect(fetches).toEqual([])
    })

    it('yields nothing for an empty first page', async () => {
        const {iterable, fetches} = scripted(Pageable.create(0, 2),
                                             {content: [], totalElements: 0, cursor: undefined},
                                             [])

        expect(await contents(iterable)).toEqual([])
        expect(fetches).toEqual([])
    })

    it('walks every offset page and fetches none past the total', async () => {
        const {iterable, fetches} = scripted(Pageable.create(0, 2),
                                             {content: [1, 2], totalElements: 5, cursor: undefined},
                                             [{content: [3, 4], totalElements: 5, cursor: undefined},
                                              {content: [5], totalElements: 5, cursor: undefined}])

        expect(await contents(iterable)).toEqual([[1, 2], [3, 4], [5]])
        expect(fetches.map(p => (p as any).pageNumber)).toEqual([1, 2])
    })

    it('keeps offset paging going when the total is not known', async () => {
        const {iterable} = scripted(Pageable.create(0, 2),
                                    {content: [1, 2], totalElements: null, cursor: undefined},
                                    [{content: [3], totalElements: null, cursor: undefined}])

        expect(await contents(iterable)).toEqual([[1, 2], [3]])
    })

    it('yields pages that keep their own content once collected', async () => {
        const {iterable} = scripted(Pageable.create(0, 2),
                                    {content: [1, 2], totalElements: 3, cursor: undefined},
                                    [{content: [3], totalElements: 3, cursor: undefined}])

        const pages: IterablePage<number>[] = []
        for await (const page of iterable) {
            pages.push(page)
        }
        expect(pages.map(p => p.content)).toEqual([[1, 2], [3]])
        expect(pages.map(p => p.isLastPage())).toEqual([false, true])
    })

    it('leaves the caller\'s pageable unchanged', async () => {
        const offset = Pageable.create(0, 2)
        await contents(scripted(offset,
                                {content: [1, 2], totalElements: 3, cursor: undefined},
                                [{content: [3], totalElements: 3, cursor: undefined}]).iterable)
        expect(offset.pageNumber).toBe(0)

        const cursor = Pageable.createWithCursor(null, 2)
        await contents(scripted(cursor,
                                {content: [1, 2], cursor: 'c1', totalElements: null},
                                [{content: [3], cursor: null, totalElements: null}]).iterable)
        expect(cursor.cursor).toBeNull()
    })

    it('fetches each page once when next() calls overlap', async () => {
        const {iterable, fetches} = scripted(Pageable.create(0, 2),
                                             {content: [1, 2], totalElements: 5, cursor: undefined},
                                             [{content: [3, 4], totalElements: 5, cursor: undefined},
                                              {content: [5], totalElements: 5, cursor: undefined}])

        const results = await Promise.all([iterable.next(), iterable.next(), iterable.next(), iterable.next()])
        expect(results.map(r => r.done ? 'done' : r.value.content)).toEqual([[1, 2], [3, 4], [5], 'done'])
        expect(fetches.map(p => (p as any).pageNumber)).toEqual([1, 2])
    })

    it('retries the same page after a failed fetch', async () => {
        let failNext = true
        const iterable = new FunctionalIterablePage<number>(Pageable.create(0, 2),
                                                            {content: [1, 2], totalElements: 3, cursor: undefined},
                                                            async (p) => {
                                                                if(failNext){
                                                                    failNext = false
                                                                    throw new Error('transient')
                                                                }
                                                                expect((p as any).pageNumber).toBe(1)
                                                                return {content: [3], totalElements: 3, cursor: undefined}
                                                            })

        expect((await iterable.next()).value.content).toEqual([1, 2])
        await expect(iterable.next()).rejects.toThrow('transient')
        expect((await iterable.next()).value.content).toEqual([3])
        expect((await iterable.next()).done).toBe(true)
    })

})
