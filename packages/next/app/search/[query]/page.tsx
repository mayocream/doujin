import BookCard from '@/components/book-card'
import ErrorMessage from '@/components/error'
import Pagination from '@/components/pagination'
import { createClient } from '@/lib/supabase/server'

// refer: https://nextjs.org/docs/app/api-reference/file-conventions/page#props
export default async function SearchPage({
  params,
  searchParams,
}: {
  params: Promise<{ query: string }>
  searchParams: Promise<{ [key: string]: string | string[] | undefined }>
}) {
  const { query: searchQuery } = await params
  const query = decodeURIComponent(searchQuery)

  const { page } = await searchParams
  const p = Number(page) || 1

  const supbase = await createClient()
  const { count, data, error } = await supbase
    .from('books')
    .select(`*, authors (name)`, { count: 'exact' })
    .like('name', `%${query}%`)
    .order('id', { ascending: false })
    .range((p - 1) * 30, p * 30 - 1)

  if (error) {
    console.error('Error fetching search results:', error)
    return <ErrorMessage />
  }

  return (
    <main className='container mx-auto p-6'>
      <div className='mb-8 px-6'>
        <h2 className='text-3xl font-bold mb-2'>検索結果: {query}</h2>
        <div className='w-20 h-1 bg-indigo-500' />
      </div>

      <div className='flex flex-wrap gap-4'>
        {data?.map((book) => (
          <BookCard key={book.id} book={book} />
        ))}
      </div>

      <div className='flex justify-center mt-8'>
        <Pagination currentPage={p} totalPages={Math.ceil(count / 100)} />
      </div>
    </main>
  )
}
