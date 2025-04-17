import React from 'react';
import { useNavigate } from 'react-router-dom';
import LoginForm from '../components/LoginForm';

const LoginPage: React.FC = () => {
  const navigate = useNavigate();
  return <LoginForm onLogin={() => navigate('/')} />;
};

export default LoginPage; 