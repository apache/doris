import React from 'react';
import { UserInfo } from './common.interface';

export const UserInfoContext = React.createContext<UserInfo | null>(null);
