using BusComun;
using Comun;
using System.Collections.Generic;
using System.Threading;

namespace BusInchost
{
    class Hilos : cBase
    {
        public int _nElementosPorHilo = 100;
        public int _nHilos = 10;
        public List<HiloAIS> ListaHilos;

        private int nListaActual;

        /// <summary> Realiza la creacion de los hilos </summary>
        public Hilos(int nElementosPorHilo, int nHilos)
        {
            nListaActual = 0;
            _nElementosPorHilo = nElementosPorHilo;
            _nHilos = nHilos;
            ListaHilos = new List<HiloAIS>();

            HiloAIS Hilo;
            for (int i = 0; i < nHilos; i++)
            {
                Hilo = new HiloAIS();
                ListaHilos.Add(Hilo);

            }



        }

        private void Inicializar(int nElementosPorHilo, int nHilos)
        {
            nListaActual = 0;
            _nElementosPorHilo = nElementosPorHilo;
            _nHilos = nHilos;

            HiloAIS Hilo;
            for (int i = 0; i < nHilos; i++)
            {
                Hilo = ListaHilos[i];
                Hilo.ListaElementosAIS = new List<object>();

            }
        }

        /// <summary> Carga el Elemento en algún Hilo con capacidad ociosa. 
        /// Si la función retorna verdadero entonces los Hilos aún tienen capacidad ociosa.
        /// Si la función retorna falso entonces todos los Hilos están en su capacidad máxima 
        /// y debe ser ejecutados</summary>
        public bool CargoElementoEnAlgunHilo(object Elemento)
        {
            bool nRet = true;

            ListaHilos[nListaActual].ListaElementosAIS.Add(Elemento);

            if (ListaHilos[nListaActual].ListaElementosAIS.Count >= _nElementosPorHilo)
            {
                nListaActual++;
            }

            if (nListaActual == _nHilos)
            {
                nRet = false;
                nListaActual = 0;
            }

            return nRet;
        }

        /// <summary> Ejecuta los hilos  </summary>
        public bool Arrancar(ParameterizedThreadStart FuncionAEjecutar)
        {

            for (int i = 0; i < ListaHilos.Count; i++)
            {
                if (ListaHilos[i].ListaElementosAIS.Count > 0)
                {
                    ListaHilos[i].Hilo = new Thread(FuncionAEjecutar);
                    //ListaHilos[i].Hilo.Start(ListaHilos[i].ListaElementosAIS);
                    ListaHilos[i].Hilo.Start(ListaHilos[i]);
                    Thread.Sleep(10);
                }

            }
            bool Ejecutando = true;

            while (Ejecutando)
            {
                bool nRet = false;
                for (int i = 0; i < ListaHilos.Count; i++)
                {
                    if (ListaHilos[i].ListaElementosAIS.Count > 0)
                        nRet = nRet || ListaHilos[i].Hilo.IsAlive;
                    Thread.Sleep(10);
                }

                Ejecutando = nRet;
            }

            Inicializar(_nElementosPorHilo, _nHilos);
            return true;
        }

        /// <summary> Conexion de los hilos a la base de datos  </summary>
        internal bool ConectarHilosABaseDeDatos(string XMLConex)
        {

            for (int i = 0; i < ListaHilos.Count; i++)
            {
                ListaHilos[i].Conexion = new cConexion(XMLConex);
                ListaHilos[i].Conexion.Conectar();
                if (ListaHilos[i].Conexion.Errores.Cantidad() != 0)
                {
                    cIncidencia.Generar(ListaHilos[i].Conexion.Errores, "OPENBD", "No se pudo abrir la conexion para los hilos de ejecucion");
                    return false;
                }
                ListaHilos[i].Conexion.ComienzoTransaccion();
                if (ListaHilos[i].Conexion.Errores.Cantidad() != 0)
                {
                    cIncidencia.Generar(ListaHilos[i].Conexion.Errores, "OPENBD", "No se pudo abrir la conexion para los hilos de ejecucion");
                    return false;
                }

            }
            return true;
        }

        /// <summary> Desconectar los hilos de la base de datos  </summary>
        internal bool DesConectarHilosABaseDeDatos()
        {

            for (int i = 0; i < ListaHilos.Count; i++)
            {
                ListaHilos[i].Conexion.Desconectar();
                if (ListaHilos[i].Conexion.Errores.Cantidad() != 0)
                {
                    cIncidencia.Generar(ListaHilos[i].Conexion.Errores, "CLOSEBD", "Error al desconectar hilos");
                    return false;
                }

            }
            return true;
        }

        /// <summary> Commit de los hilos en la base de datos  </summary>
        internal bool CommitDeLosHilos()
        {
            bool nRet = true;

            for (int i = 0; i < ListaHilos.Count; i++)
            {
                if (ListaHilos[i].Conexion != null)
                {
                    ListaHilos[i].Conexion.CommitTransaccion();

                    if (ListaHilos[i].Conexion.Errores.Cantidad() != 0)
                    {
                        //cIncidencia.Generar(ListaHilos[i].Conexion.Errores, "COMMITBD", "Error al realizar commit de la transaccion para el hilo");
                        nRet = false;
                    }
                }
            }

            return nRet;
        }

        /// <summary> Rollback de los hilos en la base de datos  </summary>
        internal bool RollbackDeLosHilos()
        {
            bool nRet = true;

            for (int i = 0; i < ListaHilos.Count; i++)
            {
                if (ListaHilos[i].Conexion != null)
                {
                    ListaHilos[i].Conexion.RollbackTransaccion();

                    if (ListaHilos[i].Conexion.Errores.Cantidad() != 0)
                    {
                        //cIncidencia.Generar(ListaHilos[i].Conexion.Errores, "ROLBACKBD", "Error al realizar rollback de la transaccion para el hilo");
                        nRet = false;
                    }
                }
            }

            return nRet;
        }

        /// <summary> Retorna Verdadero si todos los hilos se ejecutaron correctamente</summary>
        internal bool EjecucionDeLosHilosOk()
        {
            bool nRet = true;

            for (int i = 0; i < ListaHilos.Count; i++)
            {
                if (ListaHilos[i].bEjecutadoOk != true)
                    nRet = false;
            }

            return nRet;
        }

        /// <summary> Retorna la cantidad de elementos tratados</summary>
        internal int ElemenosTratados()
        {
            int nRet = 0;

            for (int i = 0; i < ListaHilos.Count; i++)
            {
                nRet = nRet + ListaHilos[i].nContTratados;
            }

            return nRet;
        }

        /// <summary> Retorna la cantidad de elementos tratados de manera exitosa</summary>
        internal int ElemenosTratadosOk()
        {
            int nRet = 0;

            for (int i = 0; i < ListaHilos.Count; i++)
            {
                nRet = nRet + ListaHilos[i].nContOk;
            }

            return nRet;
        }
    }

    class HiloAIS : cBase
    {
        public List<object> ListaElementosAIS;
        public Thread Hilo;
        public cConexion Conexion;
        public int nContTratados;
        public int nContOk;
        public bool bEjecutadoOk;

        public HiloAIS()
        {
            ListaElementosAIS = new List<object>();
            nContTratados = 0;
            nContOk = 0;
            bEjecutadoOk = true;

        }

    }
}
