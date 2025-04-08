'use client'

import { Pagination as MPagination } from '@mui/material'
import { useRouter } from 'next/navigation'

export default function Pagination({ count, page }) {
  const router = useRouter()

  return (
    <MPagination
      count={count}
      page={page}
      color='primary'
      onChange={(_, page) => {
        router.push(`?page=${page}`)
      }}
    />
  ) // Adjust size as needed
}
