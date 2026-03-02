import { redirect } from 'next/navigation';
import { getServerSession } from 'next-auth';
import { authOptions } from '@/lib/auth';

export default async function RootPage() {
    const session = await getServerSession(authOptions);

    if (session) {
        // Usuario autenticado → ir al dashboard
        redirect('/registro');
    } else {
        // Usuario no autenticado → ir a login
        redirect('/login');
    }
}
