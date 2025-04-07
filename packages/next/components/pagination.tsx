'use client'

import { Flex, Button } from '@radix-ui/themes'
import { ChevronLeft, ChevronRight, MoreHorizontal } from 'lucide-react'
import { useRouter } from 'next/navigation'

export default function Pagination({ currentPage, totalPages }) {
  const router = useRouter()

  // Convert to numbers to ensure proper comparison
  currentPage = Number(currentPage)
  totalPages = Number(totalPages)

  const goToPage = (page) => {
    router.push(`?page=${page}`)
  }

  // Calculate which page numbers to display
  const getPageNumbers = () => {
    const pageNumbers = []
    const maxVisiblePages = 7 // Max number of page buttons (excluding ellipsis)

    // For small number of pages, show all
    if (totalPages <= maxVisiblePages) {
      for (let i = 1; i <= totalPages; i++) {
        pageNumbers.push(i)
      }
      return pageNumbers
    }

    // Always add first page
    pageNumbers.push(1)

    let leftSide = Math.max(2, currentPage - 2)
    let rightSide = Math.min(totalPages - 1, currentPage + 2)

    // Adjust if we're close to either end
    if (currentPage <= 4) {
      // Near beginning, show more pages at start
      leftSide = 2
      rightSide = Math.min(6, totalPages - 1)
    } else if (currentPage >= totalPages - 3) {
      // Near end, show more pages at end
      leftSide = Math.max(2, totalPages - 5)
      rightSide = totalPages - 1
    }

    // Add ellipsis on left if needed
    if (leftSide > 2) {
      pageNumbers.push('ellipsis-left')
    }

    // Add middle pages
    for (let i = leftSide; i <= rightSide; i++) {
      pageNumbers.push(i)
    }

    // Add ellipsis on right if needed
    if (rightSide < totalPages - 1) {
      pageNumbers.push('ellipsis-right')
    }

    // Always add last page
    pageNumbers.push(totalPages)

    return pageNumbers
  }

  // Don't render pagination if there's only one page
  if (totalPages <= 1) {
    return null
  }

  return (
    <Flex gap='2' justify='center' py='4'>
      <Button
        variant='soft'
        disabled={currentPage <= 1}
        onClick={() => goToPage(currentPage - 1)}
        aria-label='Previous page'
      >
        <ChevronLeft size={16} />
      </Button>

      {getPageNumbers().map((page, index) => {
        if (page === 'ellipsis-left' || page === 'ellipsis-right') {
          return (
            <div key={page} className='flex items-center'>
              <MoreHorizontal size={16} />
            </div>
          )
        }

        return (
          <Button
            key={page}
            variant={page === currentPage ? 'solid' : 'soft'}
            onClick={() => goToPage(page)}
            aria-current={page === currentPage ? 'page' : undefined}
          >
            {page}
          </Button>
        )
      })}

      <Button
        variant='soft'
        disabled={currentPage >= totalPages}
        onClick={() => goToPage(currentPage + 1)}
        aria-label='Next page'
      >
        <ChevronRight size={16} />
      </Button>
    </Flex>
  )
}
