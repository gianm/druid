/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import type { IconName } from '@blueprintjs/core';
import { Icon } from '@blueprintjs/core';
import classNames from 'classnames';
import type { MouseEventHandler, ReactNode } from 'react';
import React from 'react';

import './table-clickable-cell.scss';

export interface TableClickableCellProps {
  className?: string;
  onClick: MouseEventHandler<any>;
  hoverIcon?: IconName;
  title?: string;
  disabled?: boolean;
  children?: ReactNode;
}

export const TableClickableCell = React.memo(function TableClickableCell(
  props: TableClickableCellProps,
) {
  const { className, onClick, hoverIcon, disabled, children, ...rest } = props;

  return (
    <div
      className={classNames('table-clickable-cell', className, { disabled })}
      onClick={disabled ? undefined : onClick}
      {...rest}
    >
      {children}
      {hoverIcon && !disabled && <Icon className="hover-icon" icon={hoverIcon} />}
    </div>
  );
});
