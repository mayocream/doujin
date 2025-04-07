import { Card } from '@radix-ui/themes'
import { thumbnail } from '@/lib/book'

export default function BookCard({ book }) {
  return (
    <Card className='w-56 flex-shrink-0 flex-grow-0 p-4 snap-start hover:shadow-lg transition-shadow'>
      <div className='flex flex-col h-full'>
        <div className='flex justify-center'>
          <img
            className='h-64 mb-3 rounded object-cover'
            src={thumbnail(book.id)}
            alt={book.name}
          />
        </div>
        <h3 className='text-lg font-bold mb-2 break-words overflow-hidden line-clamp-2'>
          {book.name}
        </h3>
        <p className='text-sm text-gray-600 mt-auto truncate'>
          {book.authors?.map((a) => a.name).join(', ')}
        </p>
      </div>
    </Card>
  )
}
