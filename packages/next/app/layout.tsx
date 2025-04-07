import type { Metadata } from 'next'
import { Noto_Sans_JP } from 'next/font/google'
import { Theme } from '@radix-ui/themes'
import './globals.css'
import Header from '@/components/header'

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
      <body className={`${notoSansJP.className} antialiased`}>
        <Theme>
          <Header />
          {children}
        </Theme>
      </body>
    </html>
  )
}
