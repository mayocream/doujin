import type { Metadata } from 'next'
import { Noto_Sans_JP } from 'next/font/google'
import './globals.css'

const notoSansJP = Noto_Sans_JP({
  subsets: ['latin'],
})

export const metadata: Metadata = {
  title: 'Doujin',
  description: 'がんばろう同人！',
}

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode
}>) {
  return (
    <html lang='ja'>
      <body className={`${notoSansJP.className} antialiased`}>{children}</body>
    </html>
  )
}
