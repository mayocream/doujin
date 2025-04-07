import type { Metadata } from 'next'
import { Noto_Sans_JP } from 'next/font/google'
import { Theme } from '@radix-ui/themes'
import './globals.css'
import Header from '@/components/header'
import Search from '@/components/search'
import Footer from '@/components/footer'

const notoSansJP = Noto_Sans_JP({
  subsets: ['latin'],
})

export const metadata: Metadata = {
  title: '同人',
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
          <div className='flex flex-col min-h-screen gap-0 md:gap-10'>
            <Header />
            <Search />
            {children}
            <Footer />
          </div>
        </Theme>
      </body>
    </html>
  )
}
