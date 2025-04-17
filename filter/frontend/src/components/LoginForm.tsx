import React, { useState } from 'react';
import { TextField, Button, Box, Typography } from '@mui/material';

const HARDCODED_LOGIN = 'admin';
const HARDCODED_PASSWORD = 'admin123';

interface Props {
  onLogin: () => void;
}

const LoginForm: React.FC<Props> = ({ onLogin }) => {
  const [login, setLogin] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (login === HARDCODED_LOGIN && password === HARDCODED_PASSWORD) {
      localStorage.setItem('auth', '1');
      onLogin();
    } else {
      setError('Неверный логин или пароль');
    }
  };

  return (
    <Box component="form" onSubmit={handleSubmit} sx={{ maxWidth: 320, mx: 'auto', mt: 8 }}>
      <Typography variant="h5" mb={2}>Вход</Typography>
      <TextField
        label="Логин"
        value={login}
        onChange={e => setLogin(e.target.value)}
        fullWidth
        margin="normal"
      />
      <TextField
        label="Пароль"
        type="password"
        value={password}
        onChange={e => setPassword(e.target.value)}
        fullWidth
        margin="normal"
      />
      {error && <Typography color="error" variant="body2">{error}</Typography>}
      <Button type="submit" variant="contained" fullWidth sx={{ mt: 2 }}>Войти</Button>
    </Box>
  );
};

export default LoginForm; 