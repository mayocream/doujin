import { Typography } from '@mui/material'
import BookCard from '@/components/book-card'
import ErrorMessage from '@/components/error'
import { createClient } from '@/lib/supabase/server'
import Pagination from '@/components/pagination'
import { Masonry } from '@mui/lab'

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
    <div className='container mx-auto px-4 py-6'>
      <div className='mb-8 px-6'>
        <Typography variant='h4' component='h2' className='font-bold mb-2'>
          検索結果: {query}
        </Typography>
        <div className='w-20 h-1 bg-indigo-500 rounded'></div>
      </div>

      <Masonry columns={{ xs: 2, sm: 3, md: 4, lg: 6 }} spacing={2} sequential>
        {data?.map((book) => (
          <BookCard key={book.id} book={book} />
        ))}
      </Masonry>

      <div className='flex justify-center mt-8'>
        <Pagination count={Math.ceil(count / 30)} page={p} />
      </div>
    </div>
  )
}
