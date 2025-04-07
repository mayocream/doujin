'use client'

import { Flex, Button } from '@radix-ui/themes'
import { ChevronLeft, ChevronRight } from 'lucide-react'
import { useRouter } from 'next/navigation'

export default function Pagination({ currentPage, totalPages }) {
  const router = useRouter()

  const pages = []
  for (let i = 1; i <= Math.min(totalPages, 5); i++) {
    pages.push(i)
  }

  const goToPage = (page) => {
    router.push(`?page=${page}`)
  }

  return (
    <Flex gap='2' justify='center' py='4'>
      <Button
        variant='soft'
        disabled={currentPage <= 1}
        onClick={() => goToPage(currentPage - 1)}
      >
        <ChevronLeft size={16} />
      </Button>

      {pages.map((page) => (
        <Button
          key={page}
          variant={page === currentPage ? 'solid' : 'soft'}
          onClick={() => goToPage(page)}
        >
          {page}
        </Button>
      ))}

      <Button
        variant='soft'
        disabled={currentPage >= totalPages}
        onClick={() => goToPage(currentPage + 1)}
      >
        <ChevronRight size={16} />
      </Button>
    </Flex>
  )
}
