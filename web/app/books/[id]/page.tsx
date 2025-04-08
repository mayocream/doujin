import { createClient } from '@/lib/supabase/server'
import { thumbnail } from '@/lib/book'
import ErrorMessage from '@/components/error'
import { Paper, Chip, Divider } from '@mui/material'
import {
  Book as BookIcon,
  CalendarToday as CalendarIcon,
  Tag as TagIcon,
  Group as UsersIcon,
  Person as UserIcon,
  CardGiftcard as GiftIcon,
  Business as BuildingIcon,
} from '@mui/icons-material'

export default async function BookDetailPage({ params }) {
  const { id } = await params
  const supabase = await createClient()

  const { data: book, error } = await supabase
    .from('books')
    .select(
      `*,
      authors (name),
      circles (name),
      tags (name),
      genres (name),
      imprints (name),
      parodies (name),
      publishers (name),
      characters (name)
    `
    )
    .eq('id', id)
    .single()

  if (error) {
    console.error('書籍詳細の取得エラー:', error)
    return <ErrorMessage />
  }

  const InfoItem = ({ Icon, label, value }) => (
    <div className='bg-gray-50 p-3 rounded flex items-center gap-2'>
      <Icon fontSize='small' className='text-gray-600' />
      <div>
        <p className='text-xs font-medium text-gray-500'>{label}</p>
        <p className='text-sm text-gray-700'>{value}</p>
      </div>
    </div>
  )

  const CategorySection = ({ Icon, title, items, color }) =>
    items?.length > 0 && (
      <div>
        <div className='flex items-center gap-2 mb-2'>
          <Icon fontSize='small' className='text-gray-600' />
          <p className='text-sm font-medium text-gray-700'>{title}</p>
        </div>
        <div className='flex flex-wrap gap-2'>
          {items.map((item) => (
            <Chip
              key={item.name}
              label={item.name}
              size='small'
              color={color}
              variant='outlined'
            />
          ))}
        </div>
      </div>
    )

  return (
    <div className='max-w-5xl mx-auto p-4 md:p-6 bg-gray-50'>
      <div className='flex flex-col md:flex-row gap-6'>
        {/* Book Cover Column */}
        <div className='flex-shrink-0 md:w-1/3'>
          <div className='flex flex-col gap-4'>
            {/* Book Cover */}
            <Paper
              elevation={3}
              className='overflow-hidden rounded-md bg-white'
            >
              <img
                className='w-full h-full object-cover'
                src={thumbnail(book.id)}
                alt={book.name}
              />
            </Paper>

            {/* Stats */}
            <div className='flex justify-between text-sm text-gray-600 px-2'>
              <div className='flex items-center gap-1'>
                <BookIcon fontSize='small' />
                <span>
                  {Number(book.pages) > 0 && Number(book.pages)} ページ
                  {Number(book.pages) == 0 && '不詳'}
                </span>
              </div>
            </div>
          </div>
        </div>

        {/* Book Details Column */}
        <div className='flex-grow md:w-2/3'>
          <Paper className='p-5 rounded-md'>
            {/* Book Title */}
            <h1 className='text-2xl font-bold mb-2 text-gray-900'>
              {book.name}
            </h1>

            {/* Circle and Author */}
            <div className='flex flex-wrap gap-4 mb-4'>
              <div className='flex items-center gap-2'>
                <div className='bg-indigo-50 p-2 w-10 h-10 rounded-full items-center'>
                  <UsersIcon fontSize='small' className='text-blue-500' />
                </div>
                <div>
                  <p className='text-xs text-gray-500 font-medium'>サークル</p>
                  <p className='text-blue-500 font-medium'>
                    {book.circles?.map((c) => c.name).join(', ') ||
                      'サークル不明'}
                  </p>
                </div>
              </div>

              <div className='flex items-center gap-2'>
                <div className='bg-indigo-50 p-2 w-10 h-10 rounded-full items-center'>
                  <UserIcon fontSize='small' className='text-blue-500' />
                </div>
                <div>
                  <p className='text-xs text-gray-500 font-medium'>著者</p>
                  <p className='text-blue-500 font-medium'>
                    {book.authors?.map((a) => a.name).join(', ') || '著者不明'}
                  </p>
                </div>
              </div>
            </div>

            <Divider
              sx={{
                margin: '1rem 0',
              }}
            />

            {/* Book Details with Flex */}
            <div className='flex flex-wrap gap-3 mb-4'>
              {book.release_date && (
                <div className='flex-grow basis-[calc(50%-0.75rem)]'>
                  <InfoItem
                    Icon={CalendarIcon}
                    label='発売日'
                    value={book.release_date}
                  />
                </div>
              )}

              {book.isbn && (
                <div className='flex-grow basis-[calc(50%-0.75rem)]'>
                  <InfoItem Icon={BookIcon} label='ISBN' value={book.isbn} />
                </div>
              )}

              <div className='flex-grow basis-[calc(50%-0.75rem)]'>
                <InfoItem
                  Icon={TagIcon}
                  label='区分'
                  value={book.is_adult ? '成人向け' : '一般向け'}
                />
              </div>
            </div>

            <Divider
              sx={{
                margin: '1rem 0',
              }}
            />

            {/* Categories */}
            <div className='flex flex-col gap-4'>
              <CategorySection
                Icon={TagIcon}
                title='タグ'
                items={book.tags}
                color='primary'
              />

              <CategorySection
                Icon={BookIcon}
                title='ジャンル'
                items={book.genres}
                color='secondary'
              />

              <CategorySection
                Icon={UserIcon}
                title='キャラクター'
                items={book.characters}
                color='error'
              />

              <div className='flex flex-col md:flex-row flex-wrap gap-4'>
                <div className='flex-grow md:basis-[calc(50%-0.5rem)]'>
                  <CategorySection
                    Icon={GiftIcon}
                    title='パロディ元'
                    items={book.parodies}
                    color='warning'
                  />
                </div>

                <div className='flex-grow md:basis-[calc(50%-0.5rem)]'>
                  <CategorySection
                    Icon={BuildingIcon}
                    title='出版社'
                    items={book.publishers}
                    color='info'
                  />
                </div>

                <div className='flex-grow md:basis-[calc(50%-0.5rem)]'>
                  <CategorySection
                    Icon={BuildingIcon}
                    title='インプリント'
                    items={book.imprints}
                    color='success'
                  />
                </div>
              </div>
            </div>
          </Paper>
        </div>
      </div>
    </div>
  )
}
