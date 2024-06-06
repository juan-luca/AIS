using System;
using System.Collections.Generic;
using Comun;
using System.Text;

namespace Inchost
{
    class Program
    {
        /* Funcion principal de Inchost */
        static int Main(string[] args)
        {

            Inchost vInchost = new Inchost();

            return (vInchost.EjecuarInchost(args));
            
        }

    }
}
