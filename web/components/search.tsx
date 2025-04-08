'use client'

import React, { useEffect, useState } from 'react'
import {
  TextField,
  InputAdornment,
  Select,
  MenuItem,
  FormControl,
  Container,
  Paper,
  ListSubheader,
} from '@mui/material'
import SearchIcon from '@mui/icons-material/Search'
import { useRouter } from 'next/navigation'

const Search = () => {
  const router = useRouter()
  const [query, setQuery] = useState('')
  const [category, setCategory] = useState('all')

  useEffect(() => {
    if (query.length > 0) {
      router.push(`/search/${query}`)
    }
  }, [query, router])

  return (
    <Container maxWidth='md'>
      <Paper elevation={2} sx={{ p: 1, borderRadius: 1 }}>
        <TextField
          fullWidth
          variant='outlined'
          placeholder='本、著者、ジャンルなどを検索...'
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          slotProps={{
            input: {
              startAdornment: (
                <InputAdornment position='start'>
                  <SearchIcon color='primary' />
                </InputAdornment>
              ),
              endAdornment: (
                <FormControl variant='standard' sx={{ minWidth: 100 }}>
                  <Select
                    value={category}
                    onChange={(e) => setCategory(e.target.value)}
                    displayEmpty
                  >
                    <ListSubheader>カテゴリー</ListSubheader>
                    <MenuItem value='all'>すべて</MenuItem>
                    <MenuItem value='books'>本</MenuItem>
                    <MenuItem value='authors'>著者</MenuItem>
                    <MenuItem value='genres'>ジャンル</MenuItem>
                    <MenuItem value='types'>タイプ</MenuItem>
                  </Select>
                </FormControl>
              ),
            },
          }}
        />
      </Paper>
    </Container>
  )
}

export default Search
