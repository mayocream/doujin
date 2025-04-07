'use client'

import React, { useEffect, useState } from 'react'
import { TextField, Select } from '@radix-ui/themes'
import { Search as SearchIcon } from 'lucide-react'
import { useRouter } from 'next/navigation'

const Search = () => {
  const router = useRouter()
  const [query, setQuery] = useState('')

  useEffect(() => {
    if (query.length > 0) {
      router.push(`/search/${query}`)
    }
  }, [query])

  return (
    <div className='w-full max-w-3xl mx-auto'>
      <div className='flex items-center bg-white rounded-lg shadow-md p-1 hover:shadow-lg transition-all duration-300'>
        <div className='flex-1'>
          <TextField.Root
            onChange={(e) => setQuery(e.target.value)}
            variant='soft'
            className='w-full'
            placeholder='本、著者、ジャンルなどを検索...'
            size='3'
          >
            <TextField.Slot>
              <SearchIcon height={16} width={16} className='text-blue-500' />
            </TextField.Slot>

            <TextField.Slot>
              <Select.Root defaultValue='all'>
                <Select.Trigger
                  variant='ghost'
                  className='border-none bg-gray-50 hover:bg-gray-100 rounded-full px-3 transition-colors duration-200 mx-1'
                />
                <Select.Content
                  position='popper'
                  className='bg-white rounded-lg shadow-lg mt-1 overflow-hidden'
                >
                  <Select.Group>
                    <Select.Label className='text-gray-500 text-xs px-3 py-1'>
                      カテゴリー
                    </Select.Label>
                    <Select.Item value='all' className='hover:bg-gray-50'>
                      すべて
                    </Select.Item>
                    <Select.Item value='books' className='hover:bg-gray-50'>
                      本
                    </Select.Item>
                    <Select.Item value='authors' className='hover:bg-gray-50'>
                      著者
                    </Select.Item>
                    <Select.Item value='genres' className='hover:bg-gray-50'>
                      ジャンル
                    </Select.Item>
                    <Select.Item value='types' className='hover:bg-gray-50'>
                      タイプ
                    </Select.Item>
                  </Select.Group>
                </Select.Content>
              </Select.Root>
            </TextField.Slot>
          </TextField.Root>
        </div>
      </div>
    </div>
  )
}

export default Search
