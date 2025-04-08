import { Card, CardContent, CardMedia, Typography } from '@mui/material'
import { thumbnail } from '@/lib/book'
import Link from 'next/link'

export default function BookCard({ book }) {
  return (
    <Link href={`/books/${book.id}`}>
      <Card sx={{ maxWidth: 300, minWidth: 200 }}>
        <CardMedia
          component='img'
          height='200'
          image={thumbnail(book.id)}
          alt={book.name}
        />
        <CardContent>
          <Typography
            gutterBottom
            variant='subtitle1'
            component='div'
            sx={{
              overflow: 'hidden',
              display: '-webkit-box',
              WebkitLineClamp: 2,
              WebkitBoxOrient: 'vertical',
            }}
          >
            {book.name}
          </Typography>
          <Typography variant='body2' color='text.secondary' noWrap>
            {book.authors?.map((a) => a.name).join(', ')}
          </Typography>
        </CardContent>
      </Card>
    </Link>
  )
}
