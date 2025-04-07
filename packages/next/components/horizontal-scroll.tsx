'use client'

import { useRef } from 'react'
import { ChevronLeft, ChevronRight } from 'lucide-react'

export default function HorizontalScroll({ children }) {
  const scrollRef = useRef(null)

  const scroll = (direction) => {
    if (scrollRef.current) {
      const container = scrollRef.current
      const scrollAmount = direction === 'left' ? -300 : 300
      container.scrollBy({ left: scrollAmount, behavior: 'smooth' })
    }
  }

  return (
    <div className='flex items-center'>
      <button
        onClick={() => scroll('left')}
        className='flex-none -mr-4 bg-white/80 rounded-full p-2 shadow-md z-10 hover:bg-white cursor-pointer'
        aria-label='Scroll left'
      >
        <ChevronLeft className='w-6 h-6' />
      </button>

      <div
        ref={scrollRef}
        className='flex-1 overflow-x-auto flex gap-6 py-4 px-8 snap-x scrollbar-hide'
        style={{ scrollbarWidth: 'none', msOverflowStyle: 'none' }}
      >
        {children}
      </div>

      <button
        onClick={() => scroll('right')}
        className='flex-none -ml-4 bg-white/80 rounded-full p-2 shadow-md z-10 hover:bg-white cursor-pointer'
        aria-label='Scroll right'
      >
        <ChevronRight className='w-6 h-6' />
      </button>
    </div>
  )
}
