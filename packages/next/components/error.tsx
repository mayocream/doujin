import { Callout } from '@radix-ui/themes'
import { InfoIcon } from 'lucide-react'

const ErrorMessage = () => {
  return (
    <Callout.Root variant='soft' className='w-full max-w-3xl mx-auto'>
      <Callout.Icon>
        <InfoIcon height={16} width={16} className='text-blue-500' />
      </Callout.Icon>
      <Callout.Text>エラーが発生しましたわ。</Callout.Text>
    </Callout.Root>
  )
}

export default ErrorMessage
