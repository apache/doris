// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.kerberos;

import java.util.Set;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;

/**
 * JDK-only replacement for {@code io.trino.plugin.base.authentication.KerberosTicketUtils},
 * replicating its behaviour byte-for-byte so the kerberos path no longer depends on trino
 * (P3b-T01). Locates the ticket-granting ticket within a {@link Subject} and computes the
 * renew time at 80% of the ticket lifetime.
 */
public final class KerberosTicketUtils {

    // Renew when 80% of the ticket lifetime has elapsed (matches trino's TICKET_RENEW_WINDOW).
    private static final float TICKET_RENEW_WINDOW = 0.8f;

    private KerberosTicketUtils() {
    }

    public static KerberosTicket getTicketGrantingTicket(Subject subject) {
        Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);
        for (KerberosTicket ticket : tickets) {
            if (isOriginalTicketGrantingTicket(ticket)) {
                return ticket;
            }
        }
        throw new IllegalArgumentException("kerberos ticket not found in " + subject);
    }

    public static long getRefreshTime(KerberosTicket ticket) {
        long start = ticket.getStartTime().getTime();
        long end = ticket.getEndTime().getTime();
        return start + (long) ((end - start) * TICKET_RENEW_WINDOW);
    }

    public static boolean isOriginalTicketGrantingTicket(KerberosTicket ticket) {
        return isTicketGrantingServerPrincipal(ticket.getServer());
    }

    private static boolean isTicketGrantingServerPrincipal(KerberosPrincipal principal) {
        if (principal == null) {
            return false;
        }
        return principal.getName().equals("krbtgt/" + principal.getRealm() + "@" + principal.getRealm());
    }
}
