import { NextResponse } from 'next/server';
import { getToken } from 'next-auth/jwt';

// Rutas que requieren autenticación
const protectedRoutes = [
    '/registro',
    '/resumen',
    '/categorias',
    '/presupuestos',
    '/inversiones',
    '/efectivo',
    '/patrimonio',
    '/productos',
    '/importar',
];

// Rutas públicas (accesibles sin autenticación)
const publicRoutes = ['/login', '/register'];

export async function middleware(request) {
    const { pathname } = request.nextUrl;

    // Skip API routes y archivos estáticos
    if (
        pathname.startsWith('/api') ||
        pathname.startsWith('/_next') ||
        pathname.startsWith('/favicon') ||
        pathname.includes('.')
    ) {
        return NextResponse.next();
    }

    // Obtener token de sesión
    const token = await getToken({
        req: request,
        secret: process.env.NEXTAUTH_SECRET,
    });

    const isAuthenticated = !!token;
    const isProtectedRoute = protectedRoutes.some((route) => pathname.startsWith(route));
    const isPublicRoute = publicRoutes.some((route) => pathname.startsWith(route));

    // Usuario no autenticado intentando acceder a ruta protegida
    if (!isAuthenticated && isProtectedRoute) {
        const loginUrl = new URL('/login', request.url);
        loginUrl.searchParams.set('callbackUrl', pathname);
        return NextResponse.redirect(loginUrl);
    }

    // Usuario autenticado intentando acceder a login/register
    if (isAuthenticated && isPublicRoute) {
        return NextResponse.redirect(new URL('/registro', request.url));
    }

    return NextResponse.next();
}

export const config = {
    matcher: [
        /*
         * Match all request paths except:
         * - _next/static (static files)
         * - _next/image (image optimization files)
         * - favicon.ico (favicon file)
         * - public folder
         */
        '/((?!_next/static|_next/image|favicon.ico|.*\\..*|api).*)',
    ],
};
