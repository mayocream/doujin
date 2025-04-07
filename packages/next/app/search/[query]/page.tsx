import BookCard from '@/components/book-card'
import ErrorMessage from '@/components/error'
import { createClient } from '@/lib/supabase/server'

// refer: https://nextjs.org/docs/app/api-reference/file-conventions/page#props
export default async function SearchPage({
  params,
}: {
  params: Promise<{ query: string }>
}) {
  const { query } = await params

  const supbase = await createClient()
  const { data, error } = await supbase
    .from('books')
    .select(`*, authors (name)`)
    .textSearch('name', query as string)
    .order('id', { ascending: false })
    .limit(100)

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
    </main>
  )
}
