import { Alert } from '@mui/material'
import InfoIcon from '@mui/icons-material/Info'

const ErrorMessage = () => {
  return (
    <div className='w-full max-w-3xl mx-auto'>
      <Alert
        severity='info'
        icon={<InfoIcon fontSize='inherit' color='primary' />}
        sx={{ bgcolor: 'rgba(25, 118, 210, 0.08)' }}
      >
        エラーが発生しましたわ。
      </Alert>
    </div>
  )
}

export default ErrorMessage
