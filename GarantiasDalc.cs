using BusComun;
using Comun;
using System;
using System.Data;


namespace BusInchost
{

    public class TyGarantia
    {
        public string GAFECPROC;
        public string GAOPERACION;
        public string GACOD;
        public string GATIPO;
        public string GANUMERO;
        public string GAMONEDA;
        public double GAMONTO;
        public double GAVALOR;
        public string GAFECVTO;
    }

    class GarantiasDalc : cBase
    {
        public GarantiasDalc(cConexion pconn)
        {
            conn = pconn;
        }

        internal AISDataReader AbrirCursorNuevosGarantias()
        {
            AISDataReader Dr = new AISDataReader();

            string sSql = $@" SELECT GAOPERACION, GACOD, GATIPO, GANUMERO, GAMONEDA, GAMONTO, GAVALOR, GAFECVTO
FROM IN_GARA
WHERE GAFECPROC = {cFormat.StToBD(cGlobales.Hoy)}
ORDER BY GACOD ";

            Dr = conn.EjecutarDataReader(sSql);

            Errores = conn.Errores;
            return Dr;
        }

        internal bool FechGarantia(AISDataReader Dr, ref TyGarantia Garantia)
        {
            try
            {
                Garantia = new TyGarantia();
                if (Dr.Read())
                {
                    Garantia.GAOPERACION = Dr["GAOPERACION"].ToString();
                    Garantia.GACOD = Dr["GACOD"].ToString();
                    Garantia.GATIPO = Dr["GATIPO"].ToString();
                    Garantia.GANUMERO = Dr["GANUMERO"].ToString();
                    Garantia.GAMONEDA = Dr["GAMONEDA"].ToString().PadLeft(9, '0');
                    Garantia.GAMONTO = cFormat.NumBDToPC(Dr["GAMONTO"].ToString());
                    Garantia.GAVALOR = cFormat.NumBDToPC(Dr["GAVALOR"].ToString());
                    Garantia.GAFECVTO = Dr["GAFECVTO"].ToString();

                    return true;
                }
                else
                    return false;
            }
            catch (Exception e)
            {
                Errores.Agregar(Const.ERROR_BASE_DATOS, e.Message, "FechGarantia", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "FETCHGARA", "Error al recuperar Garantia");
                return false;
            }
        }

        internal TipoAccion ObtengoGarantiaBD(TyGarantia BDGarantia)
        {
            DataSet Ds = new DataSet();

            string sSql = $@" SELECT GAOPERACION, GACOD, GATIPO, GANUMERO, GAMONEDA, GAMONTO, GAVALOR, GAFECVTO 
FROM RGARANTIAS
WHERE GAOPERACION = {cFormat.StToBD(BDGarantia.GAOPERACION)}
    AND GACOD = {cFormat.StToBD(BDGarantia.GACOD)}
    AND GATIPO = {cFormat.StToBD(BDGarantia.GATIPO)}
    AND GANUMERO = {cFormat.StToBD(BDGarantia.GANUMERO)} ";

            Ds = conn.EjecutarQuery(sSql, "RGARA");

            if (conn.Errores.Cantidad() == 0)
            {
                if (Ds.Tables["RGARA"].Rows.Count > 0)
                {
                    DataRow Dr = Ds.Tables["RGARA"].Rows[0];

                    BDGarantia.GAOPERACION = Dr["GAOPERACION"].ToString();
                    BDGarantia.GACOD = Dr["GACOD"].ToString();
                    BDGarantia.GATIPO = Dr["GATIPO"].ToString();
                    BDGarantia.GANUMERO = Dr["GANUMERO"].ToString();
                    BDGarantia.GAMONEDA = Dr["GAMONEDA"].ToString();
                    BDGarantia.GAMONTO = cFormat.NumBDToPC(Dr["GAMONTO"].ToString());
                    BDGarantia.GAVALOR = cFormat.NumBDToPC(Dr["GAVALOR"].ToString());
                    BDGarantia.GAFECVTO = Dr["GAFECVTO"].ToString();

                    return TipoAccion.Modificacion;
                }
                else
                    return TipoAccion.Alta;
            }
            else
            {
                cIncidencia.Generar(conn.Errores, "SELGARA", "Error en Select de Garantias. Cliente: " + BDGarantia.GACOD + " - Codigo Operacion: " + BDGarantia.GAOPERACION + " - Tipo Garantia: " + BDGarantia.GATIPO + " - Numero: " + BDGarantia.GANUMERO);
                return TipoAccion.Error;
            }
        }

        internal bool InsertaGarantiaBD(TyGarantia Garantia)
        {
            int nRet;

            string sSql = $@" INSERT INTO RGARANTIAS (GAOPERACION, GACOD, GATIPO, GANUMERO, GAMONEDA, GAMONTO, GAVALOR, GAFECVTO)
VALUES ({cFormat.StToBD(Garantia.GAOPERACION)}, {cFormat.StToBD(Garantia.GACOD)}, {cFormat.StToBD(Garantia.GATIPO)}, 
{cFormat.StToBD(Garantia.GANUMERO)}, {cFormat.StToBD(Garantia.GAMONEDA)}, {cFormat.NumToBD(Garantia.GAMONTO.ToString())}, 
{cFormat.NumToBD(Garantia.GAVALOR.ToString())}, {cFormat.StToBD(Garantia.GAFECVTO)}) ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "InsertaGarantiaBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSGARA", "Error al insertar Garantia. Cliente: " + Garantia.GACOD + " - Codigo Operacion: " + Garantia.GAOPERACION + " - Tipo Garantia: " + Garantia.GATIPO + " - Numero: " + Garantia.GANUMERO);
                return false;
            }
        }

        internal bool ModificaGarantiaBD(TyGarantia Garantia)
        {
            int nRet;
            string sSql = $@" UPDATE RGARANTIAS SET
GAMONEDA = {cFormat.StToBD(Garantia.GAMONEDA)}, 
GAMONTO = {cFormat.NumToBD(Garantia.GAMONTO.ToString())}, 
GAVALOR = {cFormat.NumToBD(Garantia.GAVALOR.ToString())}, 
GAFECVTO = {cFormat.StToBD(Garantia.GAFECVTO)}
WHERE GAOPERACION = {cFormat.StToBD(Garantia.GAOPERACION)}
    AND GACOD = {cFormat.StToBD(Garantia.GACOD)}
    AND GATIPO = {cFormat.StToBD(Garantia.GATIPO)}
    AND GANUMERO = {cFormat.StToBD(Garantia.GANUMERO)} ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ModificaGarantiaBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDGARA", "Error al modificar Garantia. Cliente: " + Garantia.GACOD + " - Codigo Operacion: " + Garantia.GAOPERACION + " - Tipo Garantia: " + Garantia.GATIPO + " - Numero: " + Garantia.GANUMERO);
                return false;
            }
        }

        /// <summary>
        /// Borramos las garantías en producción que nos llegan en la tambla in
        /// </summary>
        internal void BorrarGarantias()
        {
            string sSql = $@"delete from RGARANTIAS p
where EXISTS (select 1 from IN_GARA i 
             where  i.GAFECPROC = '{cGlobales.Hoy}' AND i.GAOPERACION = p.GAOPERACION 
                    AND i.GACOD = p.GACOD
                    AND i.GATIPO = p.GATIPO
                    AND i.GANUMERO = p.GANUMERO) ";

            conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() > 0)
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ModificaGarantiaBD", Const.SEVERIDAD_Alta);
            }
        }

        /// <summary>
        /// Inserción masiva de garantías
        /// </summary>
        internal void InsertaGarantia()
        {
            string sSql = $@"insert into RGARANTIAS (GAOPERACION, GACOD, GATIPO, GANUMERO, GAMONEDA, GAMONTO, GAVALOR, GAFECVTO)
select GAOPERACION, GACOD, GATIPO, GANUMERO, GAMONEDA, GAMONTO, GAVALOR, GAFECVTO
from IN_GARA
where GAFECPROC = '{cGlobales.Hoy}' ";

            int res = conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() > 0)
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "InsertaGarantia", Const.SEVERIDAD_Alta);
            }
            else
            {
                cIncidencia.Aviso($"Garantias tratadas: {res}");
            }
        }
    }
}
