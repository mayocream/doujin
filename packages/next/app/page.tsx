import { createClient } from '@/lib/supabase/server'
import HorizontalScroll from '@/components/horizontal-scroll'
import BookCard from '@/components/book-card'

export default async function Index() {
  const supabase = await createClient()
  const { data: latestBooks } = await supabase
    .from('books')
    .select(
      `*,
      authors (name)
      `
    )
    .order('id', { ascending: false })
    .limit(10)

  return (
    <main className='container mx-auto px-6 py-12'>
      <div className='mb-8'>
        <h2 className='text-3xl font-bold mb-2'>新着同人誌</h2>
        <div className='w-20 h-1 bg-indigo-500' />
      </div>

      <HorizontalScroll>
        {latestBooks?.map((book) => (
          <BookCard key={book.id} book={book} />
        ))}
      </HorizontalScroll>
    </main>
  )
}
