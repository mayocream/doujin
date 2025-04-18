import Image from 'next/image'
import Link from 'next/link'

const Header = () => {
  return (
    <header className='shadow'>
      <div className='container mx-auto px-6'>
        <div className='flex justify-center items-center h-16'>
          <Link href='/' className='flex items-center'>
            <Image
              src={'/assets/images/logo.png'}
              alt='Logo'
              width={36}
              height={36}
            />
          </Link>
        </div>
      </div>
    </header>
  )
}

export default Header
