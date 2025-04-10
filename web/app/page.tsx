import { createClient } from '@/lib/supabase/server'
import HorizontalScroll from '@/components/horizontal-scroll'
import BookCard from '@/components/book-card'
import ErrorMessage from '@/components/error'
import { Masonry } from '@mui/lab'

export default async function Index() {
  const supabase = await createClient()
  const { data: latestBooks, error } = await supabase
    .from('books')
    .select(
      `*,
      authors (name)
      `
    )
    .order('release_date', { ascending: false })
    .limit(30)

  if (error) {
    console.error('Error fetching latest books:', error)
    return <ErrorMessage />
  }

  return (
    <main className='container mx-auto p-6'>
      <div className='mb-8 px-6'>
        <h2 className='text-3xl font-bold mb-2'>新着同人誌</h2>
        <div className='w-20 h-1 bg-indigo-500' />
      </div>

      <Masonry columns={{ xs: 2, sm: 3, md: 4, lg: 6 }} spacing={2} sequential>
        {latestBooks?.map((book) => (
          <BookCard key={book.id} book={book} />
        ))}
      </Masonry>
    </main>
  )
}
