import Image from 'next/image'
import Link from 'next/link'

const Header = () => {
  return (
    <header className='shadow'>
      <div className='container mx-auto px-6'>
        <div className='flex items-center h-16'>
          <div className='flex items-center'>
            <Link href='/' className='flex items-center'>
              <Image
                src={'/assets/images/logo.png'}
                alt='Logo'
                width={36}
                height={36}
                className='mr-3'
              />
            </Link>
          </div>
        </div>
      </div>
    </header>
  )
}

export default Header
